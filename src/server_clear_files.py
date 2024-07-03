"""Quick utility for cleaning out the server's job files, database, and cache directory to free up disk space."""


import os
import shutil
import time

import utils.configfile
import jobs_database
import psutil

def clean_up_server() -> None:
    """Clean up the server's job files, database, and cache directory to free up disk space."""
    jobs_db = utils.jobs_database.JobsDatabaseClient(ivert_config)
    jobs_db.download_from_s3(only_if_newer=True)