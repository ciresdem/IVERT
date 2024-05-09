## A module for managing the IVERT jobs database.

import argparse
import botocore.exceptions
import os
import re
import sqlite3
import typing

import utils.configfile
import s3


# TODO: Add support for reading/writing to an sqlite3 file.

class IvertJobsDatabaseBaseClass:
    """Base class for common operations on the IVERT jobs database.

    Consists of methods used both by the EC2-server and the IVERT client.

    Functionality specific to the EC2-server is implemented in the IvertJobsDatabaseServer class.
    """

    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabase_BaseClass class.

        Args:

        Returns:
            None
        """
        # The IVERT configfile object. Get the paths from here.
        self.ivert_config = utils.configfile.config()
        self.ivert_jobs_dir = self.ivert_config.ivert_jobs_directory_local
        self.db_fname = self.ivert_config.ivert_jobs_database_local_fname

        # The schema file.
        self.schema_file = self.ivert_config.ivert_jobs_db_schema_file

        # The database connection
        self.conn = None

        # Where the jobs database sits in the S3 bucket
        self.s3_bucket_type = self.ivert_config.s3_ivert_jobs_database_bucket_type
        self.s3_database_key = self.ivert_config.s3_ivert_jobs_database_key

        # The S3Manager instance, for uploading and downloading files to the S3 buckets
        self.s3m = s3.S3Manager()

        # The metadata key for the latest job in the database, to tag with the file in the s3 bucket.
        self.s3_latest_job_metadata_key = self.ivert_config.s3_jobs_db_latest_job_metadata_key
        self.s3_vnum_metadata_key = self.ivert_config.s3_jobs_db_version_number_metadata_key

        return

    def download_from_s3(self,
                         only_if_newer: bool = True) -> None:
        """
        Fetches the IVERT jobs database from the S3 bucket.

        Args:
            only_if_newer (bool, optional): Whether to only download if the local copy is older than the one in the S3
                bucket. Defaults to Truej. If the s3 version is newer, the local copy will be deleted before downloading
                the new verison.
            overwrite (bool, optional): Whether to overwrite the local copy if it already exists. Defaults to False.

        Returns:
            None

        Raises:
            FileNotFoundError: If the specified bucket type in S3 doesn't exist.
        """
        # Ensure that the S3Manager instance is valid
        assert isinstance(self.s3m, s3.S3Manager)

        # Get the S3 key and local database file name
        db_key = self.s3_database_key
        local_db = self.db_fname
        db_btype = self.s3_bucket_type

        # Check if the database exists in the S3 bucket
        if not self.s3m.exists(db_key, bucket_type=db_btype):
            raise FileNotFoundError(f"The {db_key} database doesn't exist in the S3 bucket.")

        if only_if_newer and os.path.exists(local_db):
            s3_vnum = self.fetch_latest_db_vnum_from_s3_metadata()
            local_vnum = self.fetch_latest_db_vnum_from_database()

            # If the s3 version number is less than the local version number, don't download.
            if s3_vnum <= local_vnum:
                return
            # If the s3 version number is greater than the local version number, delete the local version.
            else:
                os.remove(local_db)

        # Download the database from the S3 bucket
        self.s3m.download(db_key, local_db, bucket_type=db_btype)
        return

    def get_connection(self) -> sqlite3.Connection:
        """Returns the database connection for the specified database type, and intializes it if necessary.

        Returns:
            sqlite3.Connection: The database connection.
        """
        # If the connection isn't open yet, open it.
        if self.conn is None:
            # Check if the database exists
            if not self.exists(local_or_s3='local'):
                raise FileNotFoundError(f"IVERT jobs database doesn't exist in '{self.db_fname}'.")

            # Open the database
            conn = sqlite3.connect(self.db_fname)
            # We're setting row_factory to sqlite3.Row to return key-indexed dictionaries instead of tuples
            conn.row_factory = sqlite3.Row
            # Enforce foreign key constraints whenever we open the database
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.commit()
            self.conn = conn

        return self.conn

    def exists(self,
               local_or_s3: str = 'local') -> bool:
        """Checks if the database exists.

        Args:
            local_or_s3 (str): Whether to check if the "local" or "s3" database exists.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        local_or_s3 = local_or_s3.strip().lower()

        if local_or_s3 == 'local':
            return os.path.exists(self.db_fname)
        elif local_or_s3 == 's3':
            return self.s3m.exists(self.s3_database_key, bucket_type=self.s3_bucket_type)
        else:
            raise ValueError(f'local_or_s3 must be "local" or "s3". "{local_or_s3}" not recognized.')

    def __del__(self):
        """
        Commit any pending transactions and close the database connections.

        Returns:
            None
        """
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def fetch_latest_job_number_from_database(self) -> int:
        """Fetch the last job number from the database, in YYYYMMDDNNNN format.

        This is for querying the database itself, not the metdata in the S3 bucket.

        Returns:
            int: The last job number, in YYYYMMDDNNNN format.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        results = cur.execute("SELECT MAX(job_id) FROM ivert_jobs").fetchall()

        # The results is a list of sqlite.Row objects. Should be only one long of my logic is right here
        assert len(results) == 1

        resultnum = results[0]['MAX(job_id)']

        # If the result is None, it means there are no jobs in the database yet. Just return 0.
        # Else return the highest job number
        if resultnum is None:
            return 0
        else:
            return resultnum

    def fetch_latest_job_number_from_s3_metadata(self) -> typing.Union[int, None]:
        """Fetch the last job number from the S3 metadata.

        This is for querying the metadata in the S3 bucket, not the database itself.

        Returns:
            int: The last job number, in YYYYMMDDNNNN format. 0 if there are not jobs or the databasew doesn't exist in
                    the S3 bucket.

            or...

            None: If the database doesn't exist in the S3 bucket or the 'latest_job' metadata key is missing.
        """
        # If the result is None, it means there are no jobs in the database yet or it wasn't updated with the
        # latest_job number in the S3 metadata. Just return 0.
        # Else return the highest job number.
        if self.s3m.exists(self.s3_database_key, bucket_type=self.s3_bucket_type):
            md = self.s3m.get_metadata(self.s3_database_key, bucket_type=self.s3_bucket_type)
            if md is not None and self.s3_latest_job_metadata_key in md.keys():
                return int(md[self.s3_latest_job_metadata_key])
            else:
                return None
        # If the database doesn't exist in the S3 bucket, just return 0.
        else:
            return None

    def fetch_latest_db_vnum_from_database(self) -> int:
        """Fetch the latest version number from the local database.

        Returns:
            int: The last version number from the 'vnum' entry in the database.
        """
        conn = self.get_connection()
        return conn.cursor().execute("SELECT vnum FROM vnumber;").fetchall()[0]["vnum"]

    def fetch_latest_db_vnum_from_s3_metadata(self) -> typing.Union[int, None]:
        """Fetch the last version number from the S3 metadata.

        This is for querying the metadata in the S3 bucket, not the database itself.

        Returns:
            int: The last version number from the 'vnum' metadata key.

            or...

            None: If the database doesn't exist in the S3 bucket or the 'vnum' metadata key doesn't exist.
        """
        if self.s3m.exists(self.s3_database_key, bucket_type=self.s3_bucket_type):
            md = self.s3m.get_metadata(self.s3_database_key, bucket_type=self.s3_bucket_type)
            if md is not None and self.s3_vnum_metadata_key in md.keys():
                return int(md[self.s3_vnum_metadata_key])
            else:
                return None
        # If the database doesn't exist in the S3 bucket, just return None.
        else:
            return None

    def job_exists(self, username, job_id, return_row: bool = False) -> typing.Union[bool, sqlite3.Row]:
        """Returns True/False whether a (username, job_id) job presently exists in the database or not.

        If return_row is True, then it returns the sqlite3.Row object for the job if it exists.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        if return_row:
            results = cur.execute("SELECT * FROM ivert_jobs WHERE username = ? AND job_id = ?",
                                  (username, job_id)).fetchone()
            if results is None:
                return False
            else:
                return results
        else:
            cur.execute("SELECT count(*) FROM ivert_jobs WHERE username = ? AND job_id = ?", (username, job_id))
            count = cur.fetchone()[0]
            if count == 0:
                return False
            else:
                assert count == 1
                return True

    def get_params_from_s3_path(self, s3_key, bucket_type=None) -> dict:
        """Get the parameters from the S3 path.

        IVERT input-output paths are of the form "s3://[BUCKET_NAME]/[common prefix]/[command]/[username]/[YYYYMMDDNNNN]/[filename]

        Return a dictionary with the "command", "username", and "job_id" parameters.
        """
        if not bucket_type:
            bucket_type = self.s3_bucket_type
        else:
            bucket_type = bucket_type.strip().lower()

        # Separate string into parts
        s3_parts = [part for part in s3_key.split("/") if len(part) > 0]
        job_id_regex = re.compile(r'^(\d{12})$')
        # The last part should be the job ID. If this is a path to a file, remove the file part at the end.
        if not job_id_regex.match(s3_parts[-1]):
            s3_parts.pop()

        # The last part should be the job_id
        assert job_id_regex.match(s3_parts[-1])

        # 2nd-to-last part should be the username
        assert re.match("^[a-zA-Z0-9_.-]+$", s3_parts[-2])

        # 3rd-to-last part should be command
        assert s3_parts[-3] in self.ivert_config.ivert_commands

        return {"command": s3_parts[-3], "username": s3_parts[-2], "job_id": s3_parts[-1]}

    def get_job_path_from_params(self, command, username, job_id, local_os: bool = False) -> str:
        """Get the relative folder path from the parameters, outlined in ivert_config::s3_ivert_job_subdirs_template

        Args:
            command (str): The command.
            username (str): The username.
            job_id (int or str): The job ID.
            local_os (bool): Whether the path is on the local OS. Defaults to False. Only matters if we're on a Windows
                             machine with a different path separator.

        Returns:
            str: The relative folder path."""
        job_path_template = self.ivert_config.s3_ivert_job_subdirs_template
        path = job_path_template.replace("[command]", command.strip().lower()
                                         ).replace("[username]", username.strip().lower()
                                                   ).replace("[job_id]", str(job_id).strip().lower())

        if local_os:
            path = path.replace("/", os.sep)
        return path

    def delete_database(self) -> None:
        """Deletes the database locally.

        Does not touch the database in the S3 bucket.

        Returns:
            None
        """
        if self.conn:
            self.conn.close()
        os.remove(self.db_fname)

    def is_valid(self):
        """Returns True if the database is internally valid."""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("PRAGMA integrity_check;")
        resp = cursor.fetchone()[0]
        if resp.lower() == "ok":
            return True
        else:
            return False

    def is_file_in_database(self, filename: str, username: str, job_id: str) -> bool:
        """Returns True if the file is in the database."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM ivert_files WHERE filename = ? AND username = ? AND job_id = ?",
                       (filename, username, job_id))
        count = cursor.fetchone()[0]
        if count == 0:
            return False
        else:
            assert count == 1
            return True


class IvertJobsDatabaseServer(IvertJobsDatabaseBaseClass):
    """Class for managing the IVERT jobs database on the EC2 (server).

    The base class can only read and query the database. The server class can write to the database and upload it."""

    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabaseServer class.

        Args:

        Returns:
            None
        """
        super().__init__()

    def create_new_database(self,
                            only_if_not_exists_in_s3: bool = True,
                            overwrite: bool = False,
                            verbose: bool = True) -> sqlite3.Connection:
        """
        Creates a new database from scratch.

        Args:
            only_if_not_exists_in_s3 (bool): Whether to warn if the database already exists in the S3 bucket.
            overwrite (bool): Whether to overwrite the database if it already exists.
            verbose (bool): Whether to print verbose output.

        Returns:
            A sqlite connection to the newly created database.
        """
        # Make sure the schema text file exists and is a complete and valid SQL statement
        assert os.path.exists(self.schema_file)
        schema_text = open(self.schema_file, 'r').read()
        assert sqlite3.complete_statement(schema_text)

        # Create database in its location outlined in ivert_config.ini (read by the base-class constructor)
        database_fname = self.db_fname

        if only_if_not_exists_in_s3 and self.s3m.exists(self.s3_database_key, bucket_type=self.s3_bucket_type):
            raise RuntimeError('The jobs database already exists in the S3 bucket. Delete from there first:\n'
                               '> python s3.py rm {} -b {}'.format(self.s3_database_key, self.s3_bucket_type))

        if os.path.exists(database_fname):
            if overwrite:
                os.remove(database_fname)
                if verbose:
                    print(f'Delete existing {database_fname}')
            else:
                return self.get_connection()

        # Create the database
        conn = sqlite3.connect(database_fname)
        conn.row_factory = sqlite3.Row
        conn.executescript(schema_text)
        conn.execute("PRAGMA foreign_keys = ON;")

        conn.commit()

        # Assign it to the object attribute
        self.conn = conn

        # Return the connection
        return conn

    def update_job_status(self,
                          job_username: str,
                          job_id: typing.Union[int, str],
                          status: str,
                          increment_vnumber: bool = True,
                          upload_to_s3: bool = True):
        """
        Updates a job record in the existing database.

        Args:
            job_username (str): The username of the job.
            job_id (int or str): The ID of the job.
            status (str): The new status of the job.
            increment_vnumber (bool): Whether to increment the database version number. This may be set to false if
                                      several changes are being made at once, including this one.
                                      If this is set to False, then the database version number will not be incremented
                                      and the change will not be committed.
            upload_to_s3 (bool): Whether to upload the database to the S3 bucket.

        Returns:
            None
        """
        # Convert job_id to int if it's a string
        job_id = int(job_id)

        # First check to see if the job exists
        if not self.job_exists(job_username, job_id):
            raise RuntimeError(f"Job {job_username}_{job_id} does not exist in the database.")

        # Connect to the database
        conn = self.get_connection()
        cursor = conn.cursor()

        # Build the update statement
        update_stmt = """UPDATE ivert_jobs
                         SET status = ?
                         WHERE username = ? AND job_id = ?;"""

        cursor.execute(update_stmt, (status, job_username, job_id))

        # Increment the database version number
        if increment_vnumber:
            self.increment_vnumber(cursor)
            conn.commit()

        # Upload the database to the S3 bucket
        if upload_to_s3:
            self.upload_to_s3(only_if_newer=False)

        pass

    def update_file_processing_status(self, jobs_file_id: int, status: str):
        """
        Updates a file record in the existing database.

        Args:

        Returns:
            None
        """
        # TODO: Implement, and define all the possible arguments here.
        pass

    def upload_to_s3(self, only_if_newer: bool = True) -> None:
        """
        Uploads the database to the S3 bucket to be externally accessible.

        Also adds the latest_job number to the S3 metadata.

        Args:
            only_if_newer (bool): Whether to only upload the database if it's newer than the one already on S3. This
                can be used to avoid uploading the same database multiple times. It looks at the vnum parameter in the s3 metadata.

        Returns:
            None
        """
        database_vnum = self.fetch_latest_db_vnum_from_database()

        if only_if_newer:
            s3_vnum = self.fetch_latest_db_vnum_from_s3_metadata()
            # If the s3 version number is confirmed >= the local version number, then skip uploading.
            if s3_vnum is not None and database_vnum <= s3_vnum:
                return

        latest_job_id = self.fetch_latest_job_number_from_database()
        local_filename = self.db_fname
        db_s3_key = self.s3_database_key

        # Upload the database to the S3 bucket. Set the md5, the latest_job number, and the vnum in the S3 metadata.
        self.s3m.upload(local_filename,
                        db_s3_key,
                        bucket_type=self.s3_bucket_type,
                        include_md5=True,
                        other_metadata={
                            self.s3_latest_job_metadata_key: str(latest_job_id),
                            self.s3_vnum_metadata_key: str(self.fetch_latest_db_vnum_from_database())
                        },
                        )

        return

    def create_new_job(self,
                       job_s3_import_config_prefix: str,
                       job_s3_import_config_bucket_type: str = 'trusted',
                       ) -> sqlite3.Row:
        """
        Given the prefix of a job in the S3 bucket, create a new job in the "ivert_jobs" table of the database.

        If the (username, job_id) tuple already exists in the database, the job is not created but the Row is returned.
        No files are created here.

        Args:

        Returns:
            The database row (record) of the new job.
        """
        params = self.get_params_from_s3_path(job_s3_import_config_prefix)
        command = params['command']
        username = params['username']
        job_id = params['job_id']

        # Check if the (username, job_id) tuple already exists in the database.
        # If so, just return it.
        existing_row = self.job_exists(username, job_id, return_row=True)
        if existing_row:
            return existing_row

        # Get the config file.

        # TODO: Finish Implementing
        # Insert the new job into the database
        c = self.get_connection().cursor()
        c.execute("INSERT INTO ivert_jobs (command, username, job_id, import_prefix, import_bucket, configfile, ) VALUES (?, ?, ?);", (command, username, job_id))
        pass

    def create_new_file_record(self,
                               filename: str,
                               job_id: int,
                               username: str,
                               import_or_export: int):
        """
        Create a new file record in the database. The (username, job_id) tuple must already exist in the ivert_jobs table.

        Args:
            filename (str): The basename of the file being processed.
            job_id (int): The ID of the job in the "ivert_jobs" table.
            username (str): The username of the user who submitted the job.
            import_or_export (int): Whether the file is being imported (0), exported (1), or both (2).

        Returns:
            None
        """
        # TODO: Implement
        pass

    def increment_vnumber(self, cursor: typing.Union[sqlite3.Cursor, None] = None) -> None:
        """Increment the version number in the database.

        This is done every time a commit change is made to the database.

        If a cursor is provided, it will be used and
        the change will not be committed (the caller is responsible for committing the change).
        If not, a new cusor will be created and the change will be committed.

        Args:
            cursor (typing.Union[sqlite3.Cursor, None], optional): The cursor to use. Defaults to None.

        Returns:
            None
        """
        if cursor is None:
            conn = self.get_connection()
            cursor_to_use = conn.cursor()
        else:
            cursor_to_use = cursor

        cursor_to_use.execute("UPDATE vnumber SET vnum = vnum + 1;")

        if cursor is None:
            conn.commit()

    def __del__(self):
        super().__del__()

    def delete_database(self) -> None:
        """Deletes the database from the S3 bucket and locally.

        Returns:
            None
        """
        # Delete the database from the S3 bucket.
        if self.s3m.exists(self.s3_database_key, bucket_type=self.s3_bucket_type):
            try:
                self.s3m.delete(self.s3_database_key, bucket_type=self.s3_bucket_type)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'AccessDenied':
                    print("Access denied to delete the database from the S3 bucket.")
                else:
                    raise e

        # Delete the database locally using the IvertJobsDatabaseBaseClass::delete_database() method.
        super().delete_database()


def define_and_parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manipulate the ivert_jobs database.")
    parser.add_argument("command", type=str,
                        help="The command to execute. Choices: create, upload, download, delete")
    parser.add_argument("-o", "--overwrite", dest="overwrite", action="store_true",
                        help="Overwrite the existing database. (For create command only) Default: False")

    return parser.parse_args()


if __name__ == "__main__":
    args = define_and_parse_args()
    idb = IvertJobsDatabaseServer()
    if args.command == "create":
        idb.create_new_database(only_if_not_exists_in_s3=True, overwrite=args.overwrite)
    elif args.command == "upload":
        idb.upload_to_s3()
    elif args.command == "download":
        idb.download_from_s3()
    elif args.command == "delete":
        idb.delete_database()
