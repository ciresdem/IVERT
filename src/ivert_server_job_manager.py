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
import pandas
import psutil
import string
import subprocess
import sys
import time
import typing

import ivert_jobs_database
import quarantine_manager
import s3
import sns
import utils.configfile


def is_another_manager_running() -> typing.Union[bool, psutil.Process]:
    """Returns a Process if another instance of this script is already running. If none is found, returns False.

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
            input_bucket_type (str, optional): The type of S3 bucket to use for input. Defaults to "trusted".

        Returns:
            None
        """
        self.ivert_config = utils.configfile.config()
        self.time_interval_s = self.ivert_config.ivert_server_job_check_interval_s
        self.input_bucket_type = input_bucket_type
        self.input_prefix = self.ivert_config.s3_import_prefix_base

        # The jobs database object. We assume this is running on the S3 server (it doesn't make sense otherwise).
        self.jobs_db = ivert_jobs_database.IvertJobsDatabaseServer()
        self.running_jobs: list[mp.Process] = []

        self.s3m = s3.S3Manager()

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
                if isinstance(e, KeyboardInterrupt):
                    raise e

                print(f"Error: {e}", file=sys.stderr)
                print(f"Continuing to iterate. Will try again in {self.time_interval_s} seconds.", file=sys.stderr)

                time.sleep(self.time_interval_s)
                continue

    def sync_database_with_s3(self) -> None:
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

    def check_for_new_files(self) -> list[str]:
        """Return a list of new files in the trusted bucket that haven't yet been added to the database."""
        # 1. Get a list of all files in the trusted bucket.
        # 2. Filter out any files already in the database.
        # 3. Return the remaining file list.
        files_in_bucket = self.s3m.listdir(self.input_prefix, bucket_type=self.input_bucket_type, recursive=True)
        # print(files_in_bucket)

        new_files = []
        for s3_key in files_in_bucket:
            fname = s3_key.split("/")[-1]
            s3_params = self.jobs_db.get_params_from_s3_path(s3_key, bucket_type=self.input_bucket_type)
            if not self.jobs_db.file_exists(fname, s3_params['username'], s3_params['job_id']):
                new_files.append(s3_key)

        return new_files

    def check_for_new_jobs(self) -> list[str]:
        """Return a list of new .ini config files in the trusted bucket that haven't yet been added to the database.

        These indicate new IVERT jobs. Once the .ini file arrives, we can parse it and create and start the IvertJob
        object, which will handle the rest of the files and kick off the new job.."""
        # Call check_for_new_files() and get list of any new files incoming.
        # When one of these is a .ini file, kick off an IvertJob object to handle it.
        # Add it to the list of running jobs.
        new_ini_files = [fn for fn in self.check_for_new_files() if fn.lower().endswith('.ini')]
        return new_ini_files

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
                 job_config_s3_bucket_type: str = "trusted",
                 auto_start: bool = True):
        """
        Initializes a new instance of the IvertJob class.

        Args:
            job_config_s3_key (str): The S3 key of the job configuration file.
            job_config_s3_bucket_type (str): The S3 bucket type of the job configuration file. Defaults to "trusted".
            auto_start (bool): Start the job immediately upon initialization.
        """
        self.ivert_config = utils.configfile.config()

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

        # The directory where the job will be run and files stored. This will be populated and created in
        # start()-->create_local_job_folder()
        self.job_dir = ""
        self.job_config_local = ""
        self.job_config_object: typing.Union[utils.configfile.config, None] = None
        self.output_dir = ""
        self.export_prefix = ""
        self.export_bucket_type = "export"
        self.logfile = ""

        # A copy of the S3Manager used to upload and download files from the S3 bucket.
        self.s3m = s3.S3Manager()

        # The threshold time to wait for files to arrive in the trusted bucket before erroring them out and moving along.
        self.download_timeout_s = self.ivert_config.ivert_server_job_file_download_timeout_mins * 60

        # The status of the job. Generally reflected in the ivert_jobs status database.

        # These jobs are run as a subprocess, so after initialization they automatically start the processing.
        # Start the job.
        if auto_start:
            self.start()

    def start(self):
        """Start the job."""
        # TODO: Finish implementing this.

        # 1. Create local folders to store the job input files
        self.create_local_job_folders()

        # 2. Download the job configuration file from the S3 bucket.
        self.download_job_config_file()

        # 3. Parse the job configuration file.
        self.parse_job_config_ini()

        # 4. Create a new job entry in the jobs database.
        # -- also creates an ivert_files entry for the logfile, and uploads the new database version.
        self.create_new_job_entry()

        # 5. Send SNS notification that the job has started.
        #  --- Insert SNS record in database (upload to s3)
        self.push_sns_notification(start_or_finish="start")

        # 6. Download all other job files.
        #  --- Enter them each in database. (Upload to s3)
        self.download_job_files()

        # 7. Run the job!
        # -- Figure out how to monitor the status of the job as it goes along.
        self.update_job_status("running")
        self.execute_job()

        # 8. Upload export files to the S3 bucket (if any). Enter them into the database.
        self.upload_export_files()

        # 9. Upload the logfile (if exists) and enter in database. (Upload to s3)
        self.export_logfile_if_exists()

        # 10. Mark the job as finished in the jobs database. (Upload to s3)
        self.update_job_status("complete")

        # 11. Send SNS notification that the job has finished.
        self.push_sns_notification(start_or_finish="finish")

        # 12. After exporting output files, delete the local job files & folders.
        self.delete_local_job_folders()

    def create_local_job_folders(self):
        """Create a local folder to store the job input files and write output files."""
        data_basedir = self.ivert_config.ivert_jobs_directory_local
        # Make sure there's a trailing slash.
        data_basedir = data_basedir if data_basedir[-1] == "/" else data_basedir + "/"

        self.job_dir = data_basedir + self.ivert_config.s3_ivert_job_subdirs_template \
            .replace('[command]', self.command) \
            .replace('[username]', self.username) \
            .replace('[job_id]', self.job_id)

        if not os.path.exists(self.job_dir):
            os.makedirs(self.job_dir)

        self.output_dir = os.path.join(self.job_dir, "outputs")
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        # Define the export prefix.
        self.export_prefix = self.ivert_config.s3_export_prefix_base + \
                             ("" if self.ivert_config.s3_export_prefix_base[-1] == "/" else "") + \
                             (self.ivert_config.s3_ivert_job_subdirs_template \
                                  .replace('[command]', self.command) \
                                  .replace('[username]', self.username) \
                                  .replace('[job_id]', self.job_id))

        # The logfile to write output text and status messages.
        self.logfile = os.path.join(self.output_dir,
                                    self.job_config_s3_key.split("/")[-1].replace(".ini", "_log.txt"))

        return

    def delete_local_job_folders(self):
        """Remove all the job files and delete any otherwise-unused directories."""
        rm_cmd = f"rm -rf {self.job_dir}"
        subprocess.run(rm_cmd, shell=True)

        # Then peruse up the parent directories, deleting them *if* no other jobs are using that directory, there are no
        # other files present there, and it's not the top "jobs" directory that we obviously don't want to delete.
        parent_dir = os.path.dirname(self.job_dir)
        while os.path.normpath(parent_dir) != os.path.normpath(self.ivert_config.ivert_jobs_directory_local):
            contents = os.listdir(parent_dir)
            if len(contents) > 0:
                break
            else:
                os.rmdir(parent_dir)

            parent_dir = os.path.dirname(parent_dir)

    def download_job_config_file(self):
        """Download the job configuration file from the S3 bucket."""
        self.job_config_local = os.path.join(self.job_dir, self.s3_configfile_key.split("/")[-1])
        self.s3m.download(self.job_config_s3_key, self.job_config_local, bucket_type=self.job_config_s3_bucket_type)

    def parse_job_config_ini(self):
        """Parse the job configuration file, defining the IVERT job parameters."""
        # The job configfile is a .ini configuration file.
        assert os.path.exists(self.job_config_local)

        self.job_config_object = utils.configfile.config(self.job_config_local)

        if not self.is_valid_job_config(self.job_config_object):
            raise ValueError(f"Configfile {os.path.basename(self.job_config_local)} is not formatted correctly. "
                             "Does the IVERT code need to be updated?")

        return

    @staticmethod
    def is_valid_job_config(job_config_obj: utils.configfile.config) -> bool:
        """Validate to make sure the input configfile is correctly formatted and has all the necessary fields."""
        jco = job_config_obj
        if (not hasattr(jco, "username")) or type(jco.username) is not str:
            return False
        if (not hasattr(jco, "job_id")) or type(jco.job_id) is not int:
            return False
        if (not hasattr(jco, "job_name")) or type(jco.job_name) is not str:
            return False
        if (not hasattr(jco, "job_upload_prefix")) or type(jco.job_upload_prefix) is not str:
            return False
        if (not hasattr(jco, "ivert_command")) or type(jco.ivert_command) is not str:
            return False
        if (not hasattr(jco, "files")) or type(jco.files) is not list:
            return False
        if (not hasattr(jco, "cmd_args")) or type(jco.cmd_args) is not dict:
            return False

        return True

    def create_new_job_entry(self):
        """Create a new job entry in the jobs database.

        The new version of the database will be immediately uploaded."""

        assert isinstance(self.job_config_object, utils.configfile.config)
        self.jobs_db.create_new_job(self.job_config_object,
                                    self.job_config_local,
                                    self.logfile,
                                    self.job_dir,
                                    self.output_dir,
                                    job_status="started",
                                    skip_database_upload=True)

        # Generate a new file record for the config file of this job.
        self.jobs_db.create_new_file_record(self.job_config_local, self.job_id, self.username,
                                            import_or_export=0, status="processed",
                                            skip_database_upload=False)

        return

    def download_job_files(self):
        """Download all other job files from the S3 bucket and add their entries to the jobs database."""
        # It may take some time for all the files to pass quarantine and be available in the trusted bucket.
        files_to_download = self.job_config_object.files.copy()
        files_downloaded = []

        time_start = time.time()

        while len(files_to_download) > 0 and (time.time() - time_start) <= self.download_timeout_s:
            for fname in files_to_download:
                # files will each be in the same prefix as the config file.
                f_key = "/".join(self.s3_configfile_key.split("/")[:-1]) + "/" + fname
                # Put in the same local folder as the configfile.
                local_fname = os.path.join(os.path.dirname(self.job_config_local), fname)

                if self.s3m.exists(f_key, bucket_type=self.s3_configfile_bucket_type):
                    # Download from the s3 bucket.
                    self.s3m.download(f_key, local_fname, bucket_type=self.s3_configfile_bucket_type)
                    # Update our list of files to go.
                    files_to_download.remove(fname)
                    files_downloaded.append(fname)

                    # Create a file record for it in the database.
                    self.jobs_db.create_new_file_record(local_fname,
                                                        self.job_id,
                                                        self.username,
                                                        import_or_export=0,
                                                        status="downloaded",
                                                        skip_database_upload=False)

                elif quarantine_manager.is_quarantined(f_key):
                    self.jobs_db.create_new_file_record(local_fname,
                                                        self.job_id,
                                                        self.username,
                                                        import_or_export=0,
                                                        status="quarantined",
                                                        skip_database_upload=False,
                                                        fake_file_stats=True)
                    files_to_download.remove(fname)
                    files_downloaded.append(fname)

                else:
                    pass

            # If we didn't manage to download the files on the first loop, sleep 10 seconds and try again.
            if len(files_to_download) > 0:
                time.sleep(10)

        # If we've exited the loop and there are still files that didn't download, log an error status for them.
        if len(files_to_download) > 0:
            for fname in files_to_download:
                # files will each be in the same prefix as the config file.
                f_key = "/".join(self.s3_configfile_key.split("/")[:-1]) + "/" + fname
                # Put in the same local folder as the configfile.
                local_fname = os.path.join(os.path.dirname(self.job_config_local), fname)

                self.jobs_db.create_new_file_record(local_fname,
                                                    self.job_id,
                                                    self.username,
                                                    import_or_export=0,
                                                    status="timeout",
                                                    skip_database_upload=False,
                                                    fake_file_stats=True)

                files_to_download.remove(fname)
                files_downloaded.append(fname)

        assert len(files_to_download) == 0 and len(files_downloaded) == len(self.job_config_object.files)
        return

    def push_sns_notification(self, start_or_finish: str):
        """Push a SNS notification for a started or finished job.

        This notifies the IVERT user that the job has finished and that they can download the results.
        """
        # Certain jobs, we don't want to send a notification.
        # For instance "subscribe" or "unsubscribe", just let AWS do the notifying.
        if (self.command == "update") and \
                ("sub_command" in self.job_config_object.cmd_args) and \
                (self.job_config_object.cmd_args["sub_command"] in ("subscribe", "unsubscribe")):
            return

        start_or_finish = start_or_finish.strip().lower()
        if start_or_finish == "start":
            # Produce a job started notification

            email_templates = utils.configfile.config(self.ivert_config.ivert_email_templates)
            subject_line = email_templates.subject_template_job_submitted.format(self.username, self.job_id)
            body = email_templates.email_template_job_submitted.format(self.username, self.job_id, self.convert_cmd_args_to_string())

            sns.send_sns_message(subject_line, body, self.job_id, self.username)

        elif start_or_finish == "finish":
            email_templates = utils.configfile.config(self.ivert_config.ivert_email_templates)
            files_df = self.collect_job_files_df()
            # TODO: FINISH

        else:
            raise ValueError(f"parameter 'start_or_finish' must be one of 'start', or 'finish'. '{start_or_finish}' not recoginzed.")

    def collect_job_files_df(self) -> pandas.DataFrame:
        """From the ivert_files database, collect the status of all files associated with a given job, both inputs and outputs."""
        # Query the database to get the table as a pandas dataframe.
        files_df = self.jobs_db.read_table_as_pandas_df("files", self.username, self.job_id)

    def convert_cmd_args_to_string(self):
        "Convert the command arguments to a string for the purpose of sending a message to the user."
        command_str = self.job_config_object.command
        for key, val in self.job_config_object.cmd_args.items():
            command_str = command_str + f" {key}={val}"
        for fname in self.job_config_object.files:
            command_str = command_str + " " + (f'"{fname}"' if self.contains_whitespace(fname) else fname)

        return command_str

    @staticmethod
    def contains_whitespace(s):
        """Returns T/F if a string has any whitespace in it."""
        return any([wch in s for wch in string.whitespace])

    def execute_job(self):
        """Execute the job as a multiprocess sub-process. The meat of the matter.

        Monitor files that are being output."""
        if self.command == "validate":
            # RUN A VALIDATION COMMAND
            # TODO: Implement
            pass

        elif self.command == "import":
            # RUN AN IMPORT COMMAND
            # TODO: Implement
            pass

        elif self.command == "update":
            # For update, look for a particular sub-command under the args.
            assert "sub_command" in self.job_config_object.cmd_args
            sub_command = self.job_config_object.cmd_args["sub_command"]

            if sub_command == "subscribe":
                self.run_subscribe_command()

            elif sub_command == "unsubscribe":
                self.run_unsubscribe_command()

    def write_to_logfile(self, message):
        """Write out to the job's logfile."""
        if os.path.exists(self.logfile):
            f = open(self.logfile, "a")
        else:
            # Create a database record of the log file.
            self.jobs_db.create_new_file_record(self.logfile, self.job_id, self.username, import_or_export=1,
                                                status="processing", skip_database_upload=False, fake_file_stats=True)
            # Then create the file.
            f = open(self.logfile, "w")

        f.write(message)
        # Put a newline at the end of any given message, if it's not already there.
        # That way the next message will be on a new line.
        if message[-1] != '\n':
            f.write('\n')

        f.close()

    def export_logfile_if_exists(self):
        """If we've written anyting to the job's logfile, export it upon completion of the job.

        Also add an entry to the jobs_database for this logfile export."""
        if not os.path.exists(self.logfile):
            return

        # We're assuming at this point that the log-file is complete. Update the file stats in the database.
        self.jobs_db.update_file_statistics(self.username, self.job_id, self.logfile)

        self.upload_file_to_export_bucket(self.logfile)

    def upload_file_to_export_bucket(self, fname: str):
        """When exporting a file, upload it here and update the file's status in the database.

        If the file doesn't yet exist in the databse, add an entry to the jobs_database for this exported file."""
        if not os.path.exists(fname):
            return

        f_key = self.ivert_config.s3_export_prefix_base + \
                "/".join(self.s3_configfile_key.split("/")[:-1]).removeprefix(self.ivert_config.s3_import_prefix_base) + \
                "/" + os.path.basename(fname)

        # Upload the file.
        self.s3m.upload(fname, f_key, bucket_type=self.export_bucket_type)
        # Add an export file entry into the database for this job.
        if self.jobs_db.file_exists(self.username, self.job_id, fname):
            self.jobs_db.update_file_status(self.username, self.job_id, fname, "uploaded")
        else:
            self.jobs_db.create_new_file_record(fname, self.job_id, self.username, 1, status="uploaded")

        return

    def upload_export_files(self):
        """Upload any files that are set to the exported to the export bucket."""
        # Get a list of all the job files associated with this job.
        job_files_df = self.collect_job_files_df()
        # Filter the list to only include files that are marked for export.
        export_files_df = job_files_df[job_files_df["import_or_export"] >= 1]

        # If there are no files to export, do nothing.
        if len(export_files_df) == 0:
            return

        # Retrieve the local paths to the job files from the job.
        job_row = self.jobs_db.job_exists(self.username, self.job_id, return_row=True)
        assert job_row
        job_local_path = job_row["input_dir_local"]
        job_output_path = job_row["output_dir_local"]

        # Iterate over each file.
        for index, row in export_files_df.iterrows():
            f_basename = row["filename"]

            f_path = os.path.join(job_output_path, f_basename)
            f_path_other = os.path.join(job_local_path, f_basename)

            if os.path.exists(f_path):
                # Upload the file.
                self.upload_file_to_export_bucket(str(f_path))
            elif os.path.exists(f_path_other)
                # Or, upload the other file if it exists.
                self.upload_file_to_export_bucket(str(f_path_other))
            elif row["status"] in ["error", "timeout", "quarantined", "unknown"]:
                # If we'd already had a problem with this file, leave the status as it was.
                continue
            else:
                # Otherwise, change the status to 'error'. Something's wrong if we can't find the file.
                self.jobs_db.update_file_status(self.username, self.job_id, f_basename, "error")
                # Also put a message in the logfile.
                self.write_to_logfile("Error: File not found: " + f_path)

        return

    def update_job_status(self, status):
        "Update the job status in the database."
        self.jobs_db.update_job_status(self.username, self.job_id, status)

    def update_file_status(self, filename, status):
        """Update the file status in the database."""
        self.jobs_db.update_file_status(self.username, self.job_id, filename, status)

    def run_subscribe_command(self):
        "Run a 'subscribe' command to subscribe a user to an SNS notification service."
        cmd_args = self.job_config_object.cmd_args
        # If for some reason we get a 'subscribe' command where we've opted *not* to create the subscription, then just
        # quietly quit and call it a day.
        self.update_job_status("running")

        try:
            assert "email" in cmd_args
            assert "all" in cmd_args

            filter_string = None if cmd_args["all"] else self.username
            sns_arn = sns.subscribe(cmd_args["email"],
                                    filter_string)

            # Add this arn to the database. If it's already in there, update it.
            topic_arn = self.ivert_config.sns_topic_arn
            self.jobs_db.create_or_update_sns_subscription(self.username,
                                                           cmd_args["email"],
                                                           topic_arn,
                                                           sns_arn,
                                                           filter_string,
                                                           increment_vnum=False,
                                                           upload_to_s3=False
                                                           )

            self.update_job_status("complete")

        except KeyboardInterrupt as e:
            self.update_job_status("killed")
            return

        except Exception as e:
            self.write_to_logfile("ERROR encountered:\n" + str(e))
            self.update_job_status("error")
            return

        self.update_job_status("complete")
        return

    def run_unsubscribe_command(self):
        "Run a 'unsubscribe' command to unsubscribe a user from an SNS notification service."
        cmd_args = self.job_config_object.cmd_args

        try:
            assert "sub_command" in cmd_args and cmd_args["sub_command"] == "unsubscribe"
            assert "email" in cmd_args

            sns_arn = self.jobs_db.get_sns_arn(cmd_args["email"])
            sns.unsubscribe(sns_arn)

            self.jobs_db.remove_sns_subscription(cmd_args["email"], update_vnum=False, upload_to_s3=False)

        except KeyboardInterrupt as e:
            self.update_job_status("killed")
            return

        except Exception as e:
            self.write_to_logfile("ERROR encountered:\n" + str(e))
            self.update_job_status("error")
            return

        # Do the version update and the upload here.
        self.update_job_status("complete")

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
