"""A script to run an end-to-end test job of the IVERT system, without having it actually process any data.

It creates and empty .tif file, uploads it to do a validation but uses the --empty-test option to do a dry run and return a log file."""

import argparse
import os
import time

import utils.configfile
import utils.create_empty_tiff
import utils.bcolors
import client_job_upload
import client_job_download
import client_job_status
import jobs_database


def run_test_command(args: argparse.Namespace,
                     wait_time_s: int = 5) -> None:
    """Run an end-to-end test job of the IVERT system, without having it actually process any data."""

    ivert_config = utils.configfile.config()
    empty_tiff = ivert_config.empty_tiff

    # if the empty .tif doesn't already exist, just create it.
    if not os.path.exists(empty_tiff):
        utils.create_empty_tiff.create_empty_tiff()

    assert os.path.exists(empty_tiff)

    assert hasattr(args, "wait") # The 'wait' command should already be set by the test argparser.
    wait_opt = args.wait

    # Turn this into a validate command.
    val_args = argparse.Namespace(**vars(args))
    # The wait option is a local setting. Not needed for upload to the server.
    del val_args.wait
    val_args.command = "validate"
    val_args.files_or_directory = [empty_tiff]
    val_args.input_vdatum = ""
    val_args.output_vdatum = ""
    val_args.region_name = ""
    # This is the "special" flag that the validate command will get on the other side to tell it to do just a test and
    # not actually validate anything.
    val_args.EMPTY_TEST = True

    job_name = client_job_upload.upload_new_job(val_args, verbose=True)

    if wait_opt:
        print("Waiting for job to finish...")

        db = jobs_database.JobsDatabaseClient()

        while True:
            if client_job_status.is_job_finished(job_name, db):
                print()
                break
            else:
                print(".", end="", flush=True)
                time.sleep(wait_time_s)

        print("Job finished. Downloading results.")
        local_jobdir = os.path.join(ivert_config.ivert_jobs_directory_local, job_name)

        client_job_download.download_job(job_name, local_jobdir)

    else:
        print("Job has been uploaded. You can wait to receive a notification email when it's done, run "
              f"'{utils.bcolors.bcolors.BOLD}ivert status {job_name}{utils.bcolors.bcolors.ENDC}' to check the status"
              f" of the job, and then '{utils.bcolors.bcolors.BOLD}ivert download {job_name}{utils.bcolors.bcolors.ENDC}'"
              " to download the results.")


if __name__ == "__main__":
    run_test_command(argparse.Namespace(**{"command": "test", "wait": True}, wait_time_s=3))