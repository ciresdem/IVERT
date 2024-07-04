"""Quick utility for cleaning out the server's job files, database, and cache directory to free up disk space."""

import argparse
import os
import psutil
import shutil
import time
import typing

import utils.configfile
import utils.traverse_directory
import jobs_database


def clean_cache(ivert_config: typing.Union[utils.configfile.config, None] = None,
                only_if_no_running_jobs: bool = True):
    if ivert_config is None:
        ivert_config = utils.configfile.config()

    # TODO: Fix this so that it only looks for active IVERT jobs that are still running.
    #   Get the job PIDs from the jobs_database, look for ones that aren't finished in the database, and check if the procs are still running on the server.
    if only_if_no_running_jobs and len(psutil.pids()) > 0:
        return

    cache_dir = os.path.join(ivert_config.cudem_cache_directory, ".cudem_cache")
    shutil.rmtree(cache_dir, ignore_errors=True)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)


def clean_old_jobs_dirs(ivert_config: typing.Union[utils.configfile.config, None] = None):
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    jobs_files = os.listdir(ivert_config.ivert_jobs_directory_local)


def truncate_jobs_database(ivert_config: typing.Union[utils.configfile.config, None] = None,
                           save_old_data: bool = True):
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    # TODO: implement
    pass


def clean_export_dirs(ivert_config: typing.Union[utils.configfile.config, None] = None):
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    # TODO: implement
    pass


def define_and_parse_args(return_parser: bool = False):
    parser = argparse.ArgumentParser()
    parser.add_argument("--what", default="all", choices=["all", "cache", "jobs", "databaase", "export"],
                        help="What to clean from the server. 'all' means all of them. Other choices are 'cache' "
                             "(clear the .cudem_cache directory), 'jobs' (clear any local jobs directories), 'databaase' "
                             "(truncate the server's jobs database to only reflect recent jobs), and 'export' (clear the export directories."
                             " Default: 'all'")

    return parser.parse_args()

if __name__ == "__main__":
    args = define_and_parse_args()
    iconfig = utils.configfile.config()

    if args.what == "all":
        clean_cache(ivert_config=iconfig)
        clean_old_jobs_dirs(ivert_config=iconfig)
        truncate_jobs_database(ivert_config=iconfig)
        clean_export_dirs(ivert_config=iconfig)

    elif args.what == "cache":
        clean_cache(ivert_config=iconfig)

    elif args.what == "jobs":
        clean_old_jobs_dirs(ivert_config=iconfig)

    elif args.what == "databaase":
        truncate_jobs_database(ivert_config=iconfig)

    elif args.what == "export":
        clean_export_dirs(ivert_config=iconfig)
