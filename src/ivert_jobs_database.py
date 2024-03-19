## A module for managing the IVERT jobs database.

import utils.configfile

# TODO: Add support for reading/writing to an sqlite3 file.

class IvertJobsDatabaseServer:
    """Class for managing the IVERT jobs database on the EC2 (server)."""
    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabaseServer class.

        Args:

        Returns:
            None
        """
        ivert_config = utils.configfile.config()
        self.

    def create_new_database(self, database_fname: str):
        """
        Creates a new database from scratch.

        Args:
            database_fname (str): The name of the database file.

        Returns:
            None
        """
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

class IvertJobsDatabaseClient:
    """Class for managing the IVERT jobs database on the client side."""
    def __init__(self):
        """
        Initializes a new instance of the IvertJobsDatabaseClient class.

        Args:

        Returns:
            None
        """
        pass