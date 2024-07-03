"""Quick utility for cleaning out the server's job files, database, and cache directory to free up disk space."""


import os
import shutil
import time

import utils.configfile
import jobs_database
import psutil

def clear_cache(only_if_no_running_jobs=True):
    if only_if_no_running_jobs and len(psutil.pids()) > 0:
        return

    cache_dir = os.path.join(utils.configfile.config().cudem_cache_directory, ".cudem_cache")
    shutil.rmtree(cache_dir, ignore_errors=True)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

def clear_old_jobs_dir()