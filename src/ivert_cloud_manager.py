"""ivert_cloud_manager.py -- Code for managing cloud files and IVERT instances within the EC2 instance."""

# TODO: Code for identifying new jobs coming in and copying the files into the local directory

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