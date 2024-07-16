"""Quick utility for cleaning out job files, database, and cache directory to free up disk space."""

import argparse
import os
import psutil
import shutil
import time
import typing

import utils.configfile
import utils.traverse_directory
import jobs_database


def clean_cudem_cache(ivert_config: typing.Union[utils.configfile.config, None] = None,
                      only_if_no_running_jobs: bool = True,
                      verbose: bool = True):
    """Clean out the .cudem_cache directory.

    This should be run server-side only."""
    if ivert_config is None:
        ivert_config = utils.configfile.config()

    # First check to see if any active IVERT jobs are running. If so, we don't want to clean out the cache.
    if only_if_no_running_jobs:
        jobs_db = jobs_database.JobsDatabaseServer()

        running_jobs = [job for job in jobs_db.list_unfinished_jobs(return_rows=True) if psutil.pid_exists(job['job_pid'])]
        for running_job in running_jobs:
            if is_pid_an_active_ivert_job(running_job['job_pid']):
                if verbose:
                    print(f"Skipping .cudem_cache cleanup for job {running_job['job_id']}, as it is still running.")
                return

    cache_dir = os.path.join(ivert_config.cudem_cache_directory, ".cudem_cache")
    shutil.rmtree(cache_dir, ignore_errors=True)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

    return


def is_pid_an_active_ivert_job(pid: int) -> bool:
    """Check if the pid is an IVERT job.

    Checks whether 'ivert' is in the command (it should be in the path of our ivert conda environment) and 'python3' in the path.

    Args:
        pid (int): The pid to check.

    Returns:
        bool: True if the pid is an IVERT job, False otherwise."""
    try:
        proc = psutil.Process(pid)

        cmd_line_0 = proc.cmdline()[0]
        if "ivert" in cmd_line_0 and "python3" in cmd_line_0:
            return True
        else:
            return False

    except psutil.NoSuchProcess:
        return False


def are_any_ivert_jobs_running() -> bool:
    """Check if any IVERT jobs are still running.

    Returns:
        bool: True if any IVERT jobs are still running, False otherwise."""
    jobs_db = jobs_database.JobsDatabaseServer()

    running_jobs = [job for job in jobs_db.list_unfinished_jobs(return_rows=True)
                    if is_pid_an_active_ivert_job(job['job_pid'])]
    if len(running_jobs) > 0:
        return True
    else:
        return False


def fix_database_of_orphaned_jobs():
    """Fix the job status of any orphaned jobs that are no longer running on the server."""
    jobs_db = jobs_database.JobsDatabaseServer()

    open_jobs = [job for job in jobs_db.list_unfinished_jobs(return_rows=True)]

    for job in open_jobs:
        # For any jobs that are still 'unfinished' on the server but don't map to any active IVERT processes,
        # mark them as 'error'
        if (not psutil.pid_exists(job['job_pid'])) or (not is_pid_an_active_ivert_job(job['job_pid'])):
            jobs_db.update_job_status(job['username'], job['job_id'], 'error', upload_to_s3=False)

    jobs_db.upload_to_s3(only_if_newer=True)


def clean_old_jobs_dirs(ivert_config: typing.Union[utils.configfile.config, None] = None,
                        verbose: bool = True):
    """Clean out old job directories local.

    This can be run client-side or server-side."""
    if ivert_config is None:
        ivert_config = utils.configfile.config()

    if ivert_config.is_aws:
        # TODO: Clean up job directories for any no-longer-running jobs on the server side.
        pass

        jobs_files = utils.traverse_directory.list_files(ivert_config.ivert_jobs_directory_local)
        jobs_db = jobs_database.JobsDatabaseServer()

    else:
        # TODO: Clean up job directories for any no-longer-running jobs on the client side.
        jobs_subdirs = sorted([os.path.join(ivert_config.ivert_jobs_directory_local, job_dir)
                            for job_dir in os.listdir(ivert_config.ivert_jobs_directory_local)
                            if os.path.isdir(os.path.join(ivert_config.ivert_jobs_directory_local, job_dir))])

        jobs_db = jobs_database.JobsDatabaseServer()
        pass


def truncate_jobs_database(date_cutoff_str: str = "7 days ago",
                           verbose: bool = True):
    """Archive the IVERT jobs database to store only very-recent jobs & files.

    Old records will be archived on the server. This should only be run on the server side.

    Args:
        date_cutoff_str (str, optional): The date cutoff to use. Defaults to "7 days ago".
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
    """
    jobs_database.JobsDatabaseServer().archive_database(date_cutoff_str, verbose=verbose)


def clean_export_dirs(ivert_config: typing.Union[utils.configfile.config, None] = None,
                      date_cutoff_str: str = "7 days ago",
                      verbose: bool = True):
    """Clean out old files from the "export" bucket in the Secure Ingest pipeline.

    This should be run from the server side.

    Args:
        ivert_config (typing.Union[utils.configfile.config, None], optional): The IVERT config to use. Defaults to None.
        date_cutoff_str (str, optional): The date cutoff to use. Defaults to "7 days ago".
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
    """
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    # TODO: implement
    pass


def clean_untrusted_bucket(ivert_config: typing.Union[utils.configfile.config, None] = None,
                           date_cutoff_str: str = "7 days ago",
                           verbose: bool = True):
    """Clean out old files from the "untrusted" bucket in the Secure Ingest pipeline.

    This should be run from the client side.

    Args:
        ivert_config (typing.Union[utils.configfile.config, None], optional): The IVERT config to use. Defaults to None.
        date_cutoff_str (str, optional): The date cutoff to use. Defaults to "7 days ago".
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
    """
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    # TODO: implement
    pass


def define_and_parse_args(return_parser: bool = False):
    parser = argparse.ArgumentParser()
    parser.add_argument("--what", default="all", choices=["all", "cache", "jobs", "database", "export"],
                        help="What to clean from the server. 'all' means all of them. Other choices are 'cache' "
                             "(clear the .cudem_cache directory), 'jobs' (clear any local jobs directories), 'databaase' "
                             "(truncate the server's jobs database to only reflect recent jobs, or delete the database "
                             "on the client), and 'export' (clear the export directories."
                             " Default: 'all'")

    return parser.parse_args()


def delete_local_jobs_database(ivert_config: typing.Union[utils.configfile.config, None] = None,
                               verbose: bool = True):
    """Delete the local jobs database.

    Args:
        ivert_config (typing.Union[utils.configfile.config, None], optional): The IVERT config to use. Defaults to None.
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
    """
    if ivert_config is None:
        ivert_config = utils.configfile.config()

    if ivert_config.is_aws:
        raise RuntimeError("'delete_local_jobs_database()' is intended solely for IVERT client research.")
    else:
        jobs_db_fname = ivert_config.ivert_jobs_database_local_fname
        if os.path.exists(jobs_db_fname):
            if verbose:
                print(f"Deleting {os.path.basename(jobs_db_fname)}.")
            os.remove(jobs_db_fname)


if __name__ == "__main__":
    args = define_and_parse_args()
    iconfig = utils.configfile.config()

    args.what = args.what.lower().strip()

    # If we're on the server, these are our options.
    if iconfig.is_aws:
        if args.what == "all":
            clean_cudem_cache(ivert_config=iconfig)
            clean_old_jobs_dirs(ivert_config=iconfig)
            fix_database_of_orphaned_jobs()
            truncate_jobs_database()
            clean_export_dirs(ivert_config=iconfig)

        elif args.what == "cache":
            clean_cudem_cache(ivert_config=iconfig)

        elif args.what == "jobs":
            clean_old_jobs_dirs(ivert_config=iconfig)

        elif args.what == "databaase":
            fix_database_of_orphaned_jobs()
            truncate_jobs_database()

        elif args.what == "export":
            clean_export_dirs(ivert_config=iconfig)

        else:
            print(f"Argument '{args.what}' not implemented for the IVERT server.")

    # not iconfig.is_aws
    else:
        if args.what == "all":
            clean_old_jobs_dirs(ivert_config=iconfig)
            delete_local_jobs_database(ivert_config=iconfig)

        elif args.what == "jobs":
            clean_old_jobs_dirs(ivert_config=iconfig)

        elif args.what == "database":
            delete_local_jobs_database(ivert_config=iconfig)

        else:
            print(f"Argument '{args.what}' not implemented for the IVERT client.")
