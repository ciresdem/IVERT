## A module for managing the IVERT jobs database.

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
            only_if_newer (bool, optional): Whether to only download if the local copy is older than the one in the S3 bucket. Defaults to True.

        Returns:
            None

        Raises:
            FileNotFoundError: If the specified bucket type in S3 doesn't exist.

        TODO: Add support for checking only if the databse in the S3 bucket is newer than the local copy.
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

        if only_if_newer:
            header = self.s3m.get_metadata(db_key, bucket_type=db_btype)
            print(header)
            return
            # TODO: Add support for checking only if the databse in the S3 bucket is newer than the local copy.
            pass

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

    def job_exists(self, username, job_id) -> bool:
        """Returns T/F whether a (username, job_id) job presently exists in the database or not."""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT rowid FROM ivert_jobs WHERE username = ? AND job_id = ?", (username, job_id))
        data = cur.fetchone()
        if data is None:
            return False
        else:
            return True

    def get_params_from_s3_path(self, s3_key, bucket_type=None) -> dict:
        """Get the parameters from the S3 path.

        IVERT input-output paths are of the form "s3://[BUCKET_NAME]/[common prefix]/[command]/[username]/[YYYYMMDDNNNN]/[filename]

        Return a dictionary with each of these parameters.
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
        """Get the relative folder path from the parameters.

        Args:
            command (str): The command.
            username (str): The username.
            job_id (int or str): The job ID.
            local_os (bool): Whether the path is on the local OS. Defaults to False.

        Returns:
            str: The relative folder path."""
        job_path_template = self.ivert_config.ivert_job_subdirs_template
        path = job_path_template.replace("[command]", command.strip().lower()
                                         ).replace("[username]", username.strip().lower()
                                                   ).replace("[job_id]", str(job_id).strip().lower())

        if local_os:
            path = path.replace("/", os.sep)
        return path


class IvertJobsDatabaseServer(IvertJobsDatabaseBaseClass):
    """Class for managing the IVERT jobs database on the EC2 (server)."""

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

    def update_job_status(self):
        """
        Updates a job record in the existing database.

        Args:

        Returns:
            None
        """
        # TODO: Implement, and define all the possible arguments here.
        pass

    def update_file_processing_status(self):
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
        self.s3m.upload(local_filename,
                        db_s3_key,
                        bucket_type=self.s3_bucket_type,
                        include_md5=True,
                        other_metadata={
                            self.s3_latest_job_metadata_key: str(latest_job_id),
                            self.s3_vnum_metadata_key: str(self.fetch_latest_db_vnum_from_database())
                        }
                        )

        return

    def create_new_job(self,
                       job_s3_prefix: str,
                       ) -> sqlite3.Row:
        """
        Given the prefix of a job in the S3 bucket, create a new job in the "ivert_jobs" table of the database.

        If the (username, job_id) tuple already exists in the database, the job is not created but the tuple is returned.
        No files are created here.

        Args:

        Returns:
            The database row (record) of the new job.
        """
        # TODO: Implement
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

    def increment_vnumber(self):
        """Increment the version number in the database.

        This is done every time a commit change is made to the database."""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE vnumber SET vnum = vnum + 1;")
        conn.commit()

    def __del__(self):
        super().__del__()
