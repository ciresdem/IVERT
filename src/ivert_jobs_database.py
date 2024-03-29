## A module for managing the IVERT jobs database.

import os
import sqlite3

import utils.configfile
import s3


# TODO: Add support for reading/writing to an sqlite3 file.

class IvertJobsDatabase_BaseClass:
    """Base class for common operations on the IVERT jobs database."""

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
        self.db_fname_all = self.ivert_config.ivert_jobs_database_local_fname_all
        self.db_fname_latest = self.ivert_config.ivert_jobs_database_local_fname_latest

        # The schema file.
        self.schema_file = self.ivert_config.ivert_jobs_db_schema_file

        # Two databases exist: First is the "all" database, containing a record of every job run on the server.
        # The second is the "latest" database, containing the only the latest jobs run on the server.

        # The database connections and cursors
        self.conn_all = None
        self.conn_latest = None

        # Get the thresholds for the "latest" jobs database from the config.
        self.num_latest_jobs = self.ivert_config.num_latest_jobs
        self.num_latest_days = self.ivert_config.num_latest_days

        # Where the jobs database sits in the S3 bucket
        self.s3_bucket_type = self.ivert_config.s3_ivert_jobs_database_bucket_type
        self.s3_database_key_all = self.ivert_config.s3_ivert_jobs_database_key_all
        self.s3_database_key_latest = self.ivert_config.s3_ivert_jobs_database_key_latest

        self.s3m = s3.S3Manager()

        return

    def fetch_from_s3(self, full_or_latest: str = 'full'):
        """Fetches the IVERT jobs database from the S3 bucket.

        Args:
            full_or_latest (str): Whether to fetch the "full" or "latest" database.

        Returns:
            None
        """
        assert isinstance(self.s3m, s3.S3Manager)

        full_or_latest = full_or_latest.strip().lower()

        db_key = self.s3_database_key_latest if full_or_latest == 'latest' else self.s3_database_key_all
        local_db = self.db_fname_latest if full_or_latest == 'latest' else self.db_fname_all
        db_btype = self.s3_bucket_type

        if not self.s3m.bucket_exists(db_btype):
            raise ValueError(f"The {db_btype} bucket doesn't exist in the S3 bucket.")

        self.s3m.download(db_key, local_db, bucket_type=db_btype)
        return

    def get_connection(self, full_or_latest: str = 'full'):
        """Returns the database connection for the specified database type.

        Args:
            full_or_latest (str): Whether to return the "full" or "latest" database connection.

        Returns:
            sqlite3.Connection: The database connection.
        """
        full_or_latest = full_or_latest.strip().lower()

        if full_or_latest == 'full':
            db_fname = self.db_fname_all
            conn = self.conn_all
        elif full_or_latest == 'latest':
            db_fname = self.db_fname_latest
            conn = self.conn_latest
        else:
            raise ValueError('full_or_latest must be "full" or "latest"')

        # If the connection isn't open yet, open it.
        if conn is None:
            conn = sqlite3.connect(db_fname)
            conn.row_factory = sqlite3.Row
            # Enforce foreign key constraints whenever we open the database
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.commit()

            if full_or_latest == 'full':
                self.conn_all = conn
            else:
                self.conn_latest = conn

        return self.conn_all

    def exists(self,
               full_or_latest: str = 'full',
               local_or_s3: str = 'local'):
        """Checks if the database exists.

        Args:
            full_or_latest (str): Whether to check if the "full" or "latest" database exists.
            local_or_s3 (str): Whether to check if the "local" or "s3" database exists.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        full_or_latest = full_or_latest.strip().lower()
        local_or_s3 = local_or_s3.strip().lower()

        if full_or_latest == 'full':
            if local_or_s3 == 'local':
                return os.path.exists(self.db_fname_all)
            elif local_or_s3 == 's3':
                return self.s3m.exists(self.s3_database_key_all, bucket_type=self.s3_bucket_type)
            else:
                raise ValueError(f'local_or_s3 must be "local" or "s3". "{local_or_s3}" not recognized.')

        elif full_or_latest == 'latest':
            if local_or_s3 == 'local':
                return os.path.exists(self.db_fname_latest)
            elif local_or_s3 == 's3':
                return self.s3m.exists(self.s3_database_key_latest, bucket_type=self.s3_bucket_type)
            else:
                raise ValueError(f'local_or_s3 must be "local" or "s3". "{local_or_s3}" not recognized.')

        else:
            raise ValueError(f'full_or_latest must be "full" or "latest". "{full_or_latest}" not recognized.')

    def __del__(self):
        """Commit any pending transactions and close the database connections."""
        if self.conn_all:
            self.conn_all.commit()
            self.conn_all.close()
        if self.conn_latest:
            self.conn_latest.commit()
            self.conn_latest.close()


class IvertJobsDatabaseServer(IvertJobsDatabase_BaseClass):
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
            database_fname (str): The name of the database file.
            only_if_not_exists_in_s3 (bool): Whether to warn if the database already exists in the S3 bucket.
            overwrite (bool): Whether to overwrite the database if it already exists.

        Returns:
            A sqlite connection to the newly created database.
        """
        # Make sure the schema text file exists and is a complete and valid SQL statement
        assert os.path.exists(self.schema_file)
        schema_text = open(self.schema_file, 'r').read()
        assert sqlite3.complete_statement(schema_text)

        # Create database in its location outlined in ivert_config.ini (read by the base-class constructor)
        database_fname = self.ivert_jobs_file_all

        if only_if_not_exists_in_s3 and self.s3m.exists(self.s3_bucket_type, self.s3_database_key_all):
            raise RuntimeError('The jobs database already exists in the S3 bucket. Delete from there first:\n'
                               '> python s3.py rm {} -b {}'.format(self.s3_database_key_all, self.s3_bucket_type))

        if os.path.exists(database_fname):
            if overwrite:
                os.remove(database_fname)
                # TODO: FINISH HERE.
                # if os.path.exists(self.)
            else:
                return self.get_connection(full_or_latest='full')

        # Create the database
        conn = sqlite3.connect(database_fname)
        conn.row_factory = sqlite3.Row
        conn.executescript(schema_text)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.commit()

        # Assign it to the object attribute
        self.conn_all = conn

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

    def create_latest_jobs_db_from_main(self, numjobs: int = 10):
        """
        Subsets the latest jobs from the database and write the subset database to disk.

        Args:
            numjobs (int): The number of jobs to subset.

        Returns:
            None
        """
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


class IvertJobsDatabaseClient(IvertJobsDatabase_BaseClass):
    """Class for managing the IVERT jobs database on the client side."""

    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabaseClient class.

        Args:

        Returns:
            None
        """
        super().__init__()

    def __del__(self):
        super().__del__()
