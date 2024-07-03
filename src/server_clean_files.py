"""Quick utility for cleaning out the server's job files, database, and cache directory to free up disk space."""

import argparse
import os
import shutil
import time

import utils.configfile
import jobs_database
import psutil


def clean_cache(only_if_no_running_jobs=True):
    if only_if_no_running_jobs and len(psutil.pids()) > 0:
        return

    cache_dir = os.path.join(utils.configfile.config().cudem_cache_directory, ".cudem_cache")
    shutil.rmtree(cache_dir, ignore_errors=True)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)


def clean_old_jobs_dirs():
    # TODO: implement
    pass


def truncate_jobs_database(save_old_data: bool = True):
    # TODO: implement
    pass


def clean_export_dirs():
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
    if args.what == "all":
        clean_cache()
        clean_old_jobs_dirs()
        truncate_jobs_database()
        clean_export_dirs()

    elif args.what == "cache":
        clean_cache()

    elif args.what == "jobs":
        clean_old_jobs_dirs()

    elif args.what == "databaase":
        truncate_jobs_database()

    elif args.what == "export":
        clean_export_dirs()
