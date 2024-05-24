"""Utilities for checking on the status of a server-side job from the IVERT client, using the jobs_database."""

import argparse
import pandas
import typing

import utils.configfile
import jobs_database
import utils.bcolors as bcolors

ivert_config = utils.configfile.config()


def run_job_status_command(args: argparse.Namespace) -> None:
    """Run the job status command from the ivert_client."""
    assert hasattr(args, "job_name")
    assert hasattr(args, "command") and args.command == "status"
    assert hasattr(args, "detailed") is isinstance(args.detailed, bool)

    jobs_db = None

    if args.job_name.lower() == "latest":
        jobs_db = jobs_database.JobsDatabaseClient()
        args.job_name = get_most_recent_job_by_this_user(jobs_db)

    if args.detailed:
        job_df, files_df = detailed_job_info(args.job_name, jobs_db)
        print(f"Job {args.job_name} is {repr(job_df['status'].values[0])}.")
        input_files = files_df[files_df["import_or_export"].isin((0, 2))]
        export_files = files_df[files_df["import_or_export"].isin((1, 2))]

        if len(input_files) > 0:
            input_files_finished = input_files["status"].isin(("processed", "timeout", "error", "quarantined", "unknown")).sum()
            print(f"{input_files_finished} of {len(input_files)} input files are finished.")
            print()

            print("Input file statuses:")
            for i, frow in input_files.iterrows():
                print(f"    {frow['filename']}: {repr(frow['status'])}", end="")
                if frow['filename'].endswith(".ini"):
                    print(f" (<-{bcolors.bcolors.ITALIC}job config file{bcolors.bcolors.ENDC})")
                else:
                    print()

        if len(export_files) > 0:
            print(f"There are currently {len(export_files)} export files processed for this job.")

    else:
        print(f"Job {args.job_name} is {repr(get_simple_job_status(args.job_name, jobs_db))}.")


def get_most_recent_job_by_this_user(jobs_db: typing.Union[jobs_database.JobsDatabaseClient, None] = None) -> str:
    if jobs_db is None:
        jobs_db = jobs_database.JobsDatabaseClient()

    jobs_db.download_from_s3(only_if_newer=True)

    username = ivert_config.username

    # Read the jobs table, filterd by this username, and get the most recent job.
    df = jobs_db.read_table_as_pandas_df("jobs", username=username)
    return f"{username}_{df['job_id'].max()}"


def get_simple_job_status(job_name, jobs_db: typing.Union[jobs_database.JobsDatabaseClient, None] = None) -> str:
    if jobs_db is None:
        jobs_db = jobs_database.JobsDatabaseClient()

    jobs_db.download_from_s3(only_if_newer=True)

    username = job_name[:job_name.rfind("_")]
    job_id = int(job_name[job_name.rfind("_") + 1:])

    # Fetch the job status from the database.
    return jobs_db.job_status(username, job_id)


def is_job_finished(job_name, jobs_db: typing.Union[jobs_database.JobsDatabaseClient, None] = None) -> typing.Union[bool, str]:
    """Check if the job is finished.

    If it's finished, return its string status from the database.
    If not finished, return False."""
    status = get_simple_job_status(job_name, jobs_db)

    if status in ('complete', 'error', 'killed', 'unknown'):
        return True
    else:
        assert status in ('started', 'running')
        return False


def detailed_job_info(job_name, jobs_db: typing.Union[jobs_database.JobsDatabaseClient, None] = None) \
        -> list[pandas.DataFrame]:
    """Get detailed information about the status of the job.

    This returns 2 pandas dataframes: one with the job record from the ivert_jobs table,
    the other with the ivert_files table for that job."""
    if jobs_db is None:
        jobs_db = jobs_database.JobsDatabaseClient()

    jobs_db.download_from_s3(only_if_newer=True)

    username = job_name[:job_name.rfind("_")]
    job_id = int(job_name[job_name.rfind("_") + 1:])

    job_record = jobs_db.read_table_as_pandas_df("jobs", username, job_id)

    files = jobs_db.read_table_as_pandas_df("files", username, job_id)

    return [job_record, files]
