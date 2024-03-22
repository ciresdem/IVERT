## A module for managing the IVERT jobs database.

import os
import sqlite3

import utils.configfile

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
        self.ivert_jobs_file_all = os.path.join(self.ivert_jobs_dir, self.ivert_config.ivert_jobs_database_fname_all)
        self.ivert_jobs_file_latest = os.path.join(self.ivert_jobs_dir, self.ivert_config.ivert_jobs_database_fname_latest)

        # Two databases exist: First is the "all" database, containing a record of every job run on the server.
        # The second is the "latest" database, containing the only the latest jobs run on the server.

        # The database connections
        self.conn_all = None
        self.conn_latest = None

        # Get the thresholds for the "latest" jobs database from the config.
        self.num_latest_jobs = self.ivert_config.num_latest_jobs
        self.num_latest_days = self.ivert_config.num_latest_days

        return

    def __del__(self):
        if self.conn_all:
            self.conn_all.close()
        if self.conn_latest:
            self.conn_latest.close()

class IvertJobsDatabaseServer (IvertJobsDatabase_BaseClass):
    """Class for managing the IVERT jobs database on the EC2 (server)."""

    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabaseServer class.

        Args:

        Returns:
            None
        """
        super().__init__()
        self.s3_bucket = self.ivert_config.s3_ivert_jobs_database_bucket
        self.s3_database_key_all = self.ivert_config.s3_ivert_jobs_database_key_all
        self.s3_database_key_latest = self.ivert_config.s3_ivert_jobs_database_key_latest

    def create_new_database(self, database_fname: str):
        """
        Creates a new database from scratch.

        Args:
            database_fname (str): The name of the database file.

        Returns:
            None
        """
        pass

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

    def subset_latest_jobs(self, numjobs: int=10):
        """
        Subsets the latest jobs from the database and write the subset database to disk.

        Args:
            numjobs (int): The number of jobs to subset.

        Returns:
            None
        """
        pass

    def upload_databases_to_s3(self, only_if_newer: bool=True):
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

class IvertJobsDatabaseClient (IvertJobsDatabase_BaseClass):
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
