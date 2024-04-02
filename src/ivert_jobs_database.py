## A module for managing the IVERT jobs database.

import os
import sqlite3

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

        # Two databases exist: First is the "all" database, containing a record of every job run on the server.
        # The second is the "latest" database, containing the only the latest jobs run on the server.

        # The database connection
        self.conn = None

        # Where the jobs database sits in the S3 bucket
        self.s3_bucket_type = self.ivert_config.s3_ivert_jobs_database_bucket_type
        self.s3_database_key = self.ivert_config.s3_ivert_jobs_database_key

        self.s3m = s3.S3Manager()

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

        self.s3m.download(db_key, local_db, bucket_type=db_btype)
        return

    def get_connection(self) -> sqlite3.Connection:
        """Returns the database connection for the specified database type, and intializes it if necessary.

        Returns:
            sqlite3.Connection: The database connection.
        """
        # If the connection isn't open yet, open it.
        if self.conn is None:
            conn = sqlite3.connect(self.db_fname)
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

    def fetch_last_job_number(self) -> int:
        """Fetch the last job number from the database, in YYYYMMDDNNNN format.

        Returns:
            int: The last job number.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(job_number) FROM all_jobs")

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


    def update_jobs_database(self, database_fname: str):
        """
        Updates a record in the existing database.

        Args:
            database_fname (str): The name of the database file.

        Returns:
            None
        """
        # TODO: Implement, and define all the possible arguments here. This will be a base method that other methods use.
        pass


    def upload_databases_to_s3(self, only_if_newer: bool = True):
        """
        Uploads the databases to the S3 bucket to be externally accessible.

        Args:
            only_if_newer (bool): Whether to only upload the database if it's newer than the one already on S3.

        Returns:
            None
        """
        pass

    def __del__(self):
        super().__del__()
