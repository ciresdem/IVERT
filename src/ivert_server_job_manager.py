"""ivert_server_job_manager.py -- Code for managing and running IVERT jobs on an EC2 instance.

This should be always running, and there should only ever be ONE instance of it. When it kicks off it will test if
there are any other ivert_server_job_manager.py instances running. If so, it will exit.

Created by: Mike MacFerrin
April 22, 2024
"""

import argparse
import multiprocessing as mp
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
                 input_bucket_type: str = "trusted"):
        """
        Initializes a new instance of the IvertJobManager class.

        Args:

        Returns:
            None
        """
        self.ivert_config = utils.configfile.config()
        self.time_interval_s = self.ivert_config.ivert_server_job_check_interval_s
        self.input_bucket_type = input_bucket_type

        # The jobs database object. We assume this is running on the S3 server (it doesn't make sense otherwise).
        self.jobs_db = ivert_jobs_database.IvertJobsDatabaseServer()
        self.running_jobs: list[mp.Process] = []

    def start(self):
        """Start the job manager.

        This is a persistent process and should be run in the background.

        Will quietly exit if another instance of this script is already running.
        """
        # Check to see if another instance of this script is already running
        if is_another_manager_running():
            return

        self.sync_database_with_s3()

        while True:
            try:
                # First, look for new jobs arriving in the trusted bucket. (.ini files specifically)
                new_ini_keys = self.check_for_new_jobs()

                # If there are new jobs, start them in the background
                for ini_name in new_ini_keys:
                    self.start_new_job(ini_name)

                # Loop throug the list of running jobs and clean them up if they're finished.
                self.check_on_running_jobs()

                time.sleep(self.time_interval_s)

            except Exception as e:
                # If something crashes for any reason, sleep a bit and try again.
                # TODO: Implement logging if errors pop up.
                time.sleep(self.time_interval_s)
                continue

    def sync_database_with_s3(self):
        """Sync the jobs database with the S3 bucket.

        This is called only once when start() is called, to make sure the "best" version of the database is being used."""
        # Check to see if the jobs database exists, locally and/or in the S3 bucket.
        # If not, create it.
        db = self.jobs_db
        db_exists_local = False
        db_exists_s3 = False

        if db.exists('local'):
            if not db.is_valid():
                db.delete_database()
            else:
                db_exists_local = True

        if db.exists('s3'):
            db_exists_s3 = True

        # If the database doesn't exist in either place, create it.
        if not db_exists_local and not db_exists_s3:
            db.create_new_database()
            db.upload_to_s3()

        # If it only exists locally, upload it.
        elif db_exists_local and not db_exists_s3:
            db.upload_to_s3()

        # If it only exists in the s3, download it.
        elif not db_exists_local and db_exists_s3:
            db.download_from_s3()

        # If it exists in both places, use whichever one has the latest version number.
        else:
            assert db_exists_local and db_exists_s3
            # Check to see if they're both the same version number.
            local_vnum = db.fetch_latest_db_vnum_from_database()
            s3_vnum = db.fetch_latest_db_vnum_from_s3_metadata()
            if local_vnum == s3_vnum:
                return
            elif local_vnum < s3_vnum:
                db.download_from_s3(only_if_newer=True)
            else:
                db.upload_to_s3()

        return

    def check_for_new_files(self, input_bucket_type: str = "trusted") -> list[str]:
        """Return a list of new files in the trusted bucket that haven't yet been added to the database."""
        # 1. Get a list of all files in the trusted bucket.
        # 2. Filter out any files already in the database.
        # 3. Return the remaining file list.
        # TODO: Implement this.
        return []

    def check_for_new_jobs(self, input_bucket_type: str = "trusted") -> list[str]:
        """Return a list of new .ini config files in the trusted bucket that haven't yet been added to the database.

        These indicate new IVERT jobs. Once the .ini file arrives, we can parse it and create and start the IvertJob
        object, which will handle the rest of the files and kick off the new job.."""
        # Call check_for_new_files() and get list of any new files incoming.
        # When one of these is a .ini file, kick off an IvertJob object to handle it.
        # Add it to the list of running jobs.
        # TODO: Implement this.
        return []

    def check_on_running_jobs(self):
        """Check on all running jobs to see if they are still running.

        Any jobs that aren't, clean them up."""
        # TODO: Implement this.
        # Go through the list of running jobs and check if they're still running.
        # If they're not running, see if they finished successfully.
        # If so, just delete them from the list. We assume the user was already notified that the job finished.
        # If not, grab the existing log file (if exists), append any stdout/stderr from the job, and export it.
        #   Also update the job status in the database.
        #   Then send a notification to the user that the job finished unsuccessfully.

    def start_new_job(self, ini_s3_key: str):
        """Start a new job."""
        subproc = IvertJob
        proc_args = (ini_s3_key, self.input_bucket_type)
        job = mp.Process(target=subproc, args=proc_args)
        job.start()

        self.running_jobs.append(job)

    def clean_up_terminated_job(self, pid: int):
        """If a job is no longer running, clean it up, including its files.

        Fetch the job details from the database."""
        # TODO: Implement this.

    def __del__(self):
        """Clean up any running jobs."""
        for job in self.running_jobs:
            if job.is_alive():
                # Kill the IVERT job and all its children. Kinda morbid terminoligy I know, but it's what's needed here.
                parent = psutil.Process(job.pid)
                for child in parent.children(recursive=True):
                    child.kill()
                job.kill()
            if not job.is_alive():
                job.close()

            self.clean_up_terminated_job(job.pid)

        self.running_jobs = []
        return

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

        self.job_config_s3_key = job_config_s3_key
        self.job_config_s3_bucket_type = job_config_s3_bucket_type

        self.pid = os.getpid()

        # These jobs are run as a subprocess, so after initialization they automatically start the processing.
        # Start the job.
        self.start()

    def start(self):
        """Start the job."""
        # TODO: Implement this.

        # 1. Create local folders to store the job input files
        # 2. Download the job configuration file from the S3 bucket.
        # 3. Parse the job configuration file.
        # 4. Create a new job entry in the jobs database.
        # 5. Create entry for the config-file in the jobs database. (Upload to s3)
        # 6. Download all other job files.
        #  --- Enter them each in database. (Upload to s3)
        # 7. Send SNS notification that the job has started.
        #  --- Insert SNS record in database (upload to s3)
        # 8. Determine what type of job we're running.
        #  --- Does it need to export any files?
        # 9. (If needed) create local folder for job outputs.
        # 10. Run the job!
        # 11. Determine from input args, which files we need to upload to the S3 bucket to export (if any)
        # 12. Upload those files to the S3 bucket (if any)
        # 13. Enter exported files into the jobs database. (Upload to s3)
        # 14. Generate job log file, also upload to S3 and enter in database (upload to s3)
        # 14. Mark the job as finished in the jobs database. (Upload to s3)
        # 15. Send SNS notification that the job has finished.
        # 16. Delete the local job files & folders.

        # TODO: Create a new job entry in the jobs database.

    def parse_job_config_ini(self):
        """Parse the job configuration file, defining the IVERT job parameters."""
        # TODO: Implement this.

    def push_sns_notification(self, start_or_finish: str):
        """Push a SNS notification for a started or finished job.

        This notifies the IVERT user that the job has finished and that they can download the results.
        """
        # TODO: Implement this.


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
