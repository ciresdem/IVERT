"""ivert_job_manager.py -- Code for managing and running IVERT jobs on an EC2 instance.

This should be always running, and there should only ever be ONE instance of it. When it kicks off it will test if
there are any other ivert_job_manager.py instances running. If so, it will exit.

Created by: Mike MacFerrin
April 22, 2024
"""

import argparse
import numpy
import os
import psutil
import time
import typing

import ivert_jobs_database
import s3
import utils.configfile


def is_another_manager_running() -> typing.Union[bool, psutil.Process]:
    """Returns True if another instance of this script is already running.

    Returns:
        The psuil.Process object if another instance of this script is already running. False otherwise."""

    processes = psutil.process_iter()
    for p in processes:
        if "python" in p.name() and numpy.any([opt.endswith(os.path.basename(__file__)) for opt in p.cmdline()]):
            if p.pid != os.getpid():
                return p

    return False


class IvertJobManager:
    """Class for managing and running IVERT jobs on an EC2 instance.

    This will be initialized by the ivert_setup.sh script in the ivert_setup repository, using a supervisord process."""

    def __init__(self,
                 input_bucket_type: str = "trusted",
                 time_interval_s: int = 100):
        """
        Initializes a new instance of the IvertJobManager class.

        Args:

        Returns:
            None
        """
        self.time_interval_s = time_interval_s
        self.ivert_config = utils.configfile.config()
        self.input_bucket_type = input_bucket_type

        # The jobs database object. We assume this is running on the S3 server (it doesn't make sense otherwise).
        self.jobs_db = ivert_jobs_database.IvertJobsDatabaseServer()

    def start(self):
        """Start the job manager.

        This is a persistent process and should be run in the background.

        Will quietly exit if another instance of this script is already running.
        """
        # Check to see if another instance of this script is already running
        if is_another_manager_running():
            return

        # Check to see if the jobs database exists, locally and/or in the S3 bucket.
        # If not, create it.
        # TODO: Implement checking for the database and creating the jobs database locally and then on the S3 bucket.
        #  First, check if the databse exists locally (also check that it's valid.)
        #  Then, check if it exists in the S3 bucket.
        #  If both, check which one is newer, and delete the older one.
        #  If exists on S3, download to local.
        #  If only exists local, upload to S3.
        #  If neither, create the database locally and upload to S3.

        while True:
            time.sleep(self.time_interval_s)

            # TODO: Implement searching for new input files in the trusted bucket that aren't in the database,
            #  and kicking off new processes.


    def check_for_new_files(self, input_bucket_type: str = "trusted"):
        """Return a list of new files in the trusted bucket that haven't yet been added to the database."""
        # TODO: Implement this.

    def check_for_new_jobs(self, input_bucket_type: str = "trusted"):
        """Return a list of new .ini config files in the trusted bucket that haven't yet been added to the database.

        These indicate new IVERT jobs. Once the .ini file arrives, we can parse it and create and start the IvertJob
        object, which will handle the rest of the files and kick off the new job.."""
        # TODO: Implement this.

    def push_sns_notification_for_finished_job(self, job: IvertJob):
        """Push a SNS notification for a finished job.

        This notifies the IVERT user that the job has finished and that they can download the results.
        """
        # TODO: Implement this.


class IvertJob:
    """Class for managing and running IVERT individual jobs on an EC2 instance.

    The IvertJobManager class is always running and kicks off new IvertJob objects as needed."""

    def __init__(self,
                 job_config_s3_key: str,
                 job_config_s3_bucket_type: str = "trusted"):
        """
        Initializes a new instance of the IvertJob class.

        Args:
            job_config_s3_key (str): The S3 key of the job configuration file.
            job_config_s3_bucket_type (str): The S3 bucket type of the job configuration file. Defaults to "trusted".
        """
        self.s3_configfile_key = job_config_s3_key
        self.s3_configfile_bucket_type = job_config_s3_bucket_type

        self.jobs_db = ivert_jobs_database.IvertJobsDatabaseServer()

        # Assign the job ID and username.
        params_dict = self.jobs_db.get_params_from_s3_path(self.s3_configfile_key,
                                                           bucket_type=self.s3_configfile_bucket_type)

        self.job_id = params_dict["job_id"]
        self.username = params_dict["username"]
        self.command = params_dict["command"]


def define_and_parse_arguments() -> argparse.Namespace:
    """Defines and parses the command line arguments.

    Returns:
        An argparse.Namespace object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Ivert Job Manager... for detecting, running, and managing IVERT jobs on an EC2 instance.")
    parser.add_argument("-t", "--time_interval_s", type=int, target="time_interval_s", default=120,
                        help="The time interval in seconds between checking for new IVERT jobs. Default: 120.")
    parser.add_argument("-b", "--bucket", type=str, target="bucket", default="trusted",
                        help="The S3 bucket type to search for incoming job files. Default: 'trusted'. "
                             "Run 'python s3.py list_buckets' to see all available bucket types.")

    return parser.parse_args()


if __name__ == "__main__":
    # If another instance of this script is already running, exit.
    other_manager = is_another_manager_running()
    if other_manager:
        print(f"Process {other_manager.pid}: '{other_manager.name()} {other_manager.cmdline()}' is already running.")
        exit(0)

    # Parse the command line arguments
    args = define_and_parse_arguments()

    # Start the job manager
    JM = IvertJobManager(time_interval_s=args.time_interval_s)
    JM.start()
