"""ivert_cloud_manager.py -- Code for managing cloud files and IVERT instances within the EC2 instance."""

import utils.configfile
import s3

ivert_config = None

# TODO: Code for identifying new jobs coming in and copying the files into the local directory

def import_ivert_input_data(s3_bucket_key, local_dir, s3_bucket_type="trusted"):
    """Copies files from an S3 bucket to a local directory.

    For a list of s3 IVERT bucket types, see s3.py."""
    global ivert_config
    if ivert_config is None:
        ivert_config = utils.configfile.config()

    
    # TODO: Code

def export_ivert_output_data(local_dir, s3_bucket_key, s3_bucket_type="export"):
    """Copies files from a local directory to an S3 bucket.

    For a list of s3 IVERT bucket types, see s3.py."""
    global ivert_config
    if ivert_config is None:
        ivert_config = utils.configfile.config()
    # TODO: Code

# TODO: Code for notifying users of:
#    - A job successfully submitted and in-progress
#    - A job completed and a link from which to download files.
# Do this via email?  Running job on the local machine?

# TODO: Code for managing the "job status" database that keeps track of jobs (past and current) and their statuses.
# We will use a python sqlite3 instance (.db) to do this. It handle's concurrent writes with locking & waiting, which
# is fine for our needs.
# NOTE: Most this code will be managed in the vital_jobs_database()

# TODO: Code for cleaning up local dirctories, from both IMPORT and EXPORT directories.

# TODO: Code for jobs database