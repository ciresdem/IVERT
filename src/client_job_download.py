"""Utility for downloading results from IVERT."""

import argparse
import os

import jobs_database
import s3
import utils.configfile

ivert_config = utils.configfile.config()


def run_download_command(args: argparse.Namespace) -> list[str]:
    """Run the ivert_client download command."""
    assert hasattr(args, "job_id")
    assert hasattr(args, "username")
    assert hasattr(args, "output_dir")

    # If the username wasn't provided, get it from the config file (which gets it from the IVERT user profile.)
    if not args.username:
        args.username = ivert_config.username

    # Get the absolute path of the output directory.
    args.output_dir = os.path.abspath(os.path.expanduser(args.output_dir))

    # If we set it (by default) to just get the last job from the user, go find what the last job was.
    if args.job_id.lower() == "latest":
        db = jobs_database.JobsDatabaseClient()
        db.download_from_s3(only_if_newer=True)
        # Read the jobs table, filterd by this username, and get the most recent job.
        df = db.read_table_as_pandas_df("jobs", username=args.username)
        args.job_id = df["job_id"].max()

    # Download the job.
    job_name = f"{args.username}_{args.job_id}"
    return download_job(job_name, args.output_dir)


def find_most_recent_job_dir_from_this_machine() -> str:
    """Find the most recent job directory on this machine."""
    # Get the base IVERT jobs directory.
    ivert_jobs_dir = ivert_config.ivert_jobs_directory_local
    sub_folders = [fn for fn in os.listdir(ivert_jobs_dir) if os.path.isdir(os.path.join(ivert_jobs_dir, fn))]

    return os.path.join(ivert_jobs_dir, max(sub_folders))


def find_matching_job_dir(job_name: str) -> str:
    """Find the IVERT job directory with the given name."""
    dirname = os.path.join(ivert_config.ivert_jobs_directory_local, job_name)
    if not os.path.exists(dirname):
        raise FileNotFoundError(f"Could not find directory with name {job_name} in {ivert_config.ivert_jobs_directory_local}")

    return dirname


def download_job(job_name: str,
                 dest: str) -> list[str]:
    """Download the job results from the S3 bucket."""
    # Parse the job name in the format username_jobid
    assert job_name.find("_") != -1

    username = job_name[:job_name.rfind("_")]
    job_id = int(job_name[job_name.rfind("_") + 1:])

    # First, grab the database from the s3 bucket.
    db = jobs_database.JobsDatabaseClient()
    db.download_from_s3(only_if_newer=True) # only_if_newer ensures we only download it if there's a newer version in the s3 bucket.

    # Get the s3 prefix for downloaded files from the database
    job_row = db.job_exists(username, job_id, return_row=True)
    if not job_row:
        raise ValueError(f"Could not find job with name {job_name} in the IVERT jobs database.")

    export_prefix = job_row["export_prefix"]
    if export_prefix is None:
        return []

    export_glob_str = export_prefix + ("*" if export_prefix[-1] == "/" else "/*")

    # Download the results
    s3m = s3.S3Manager()
    return s3m.download(export_glob_str, dest, bucket_type="export", progress_bar=True)


def define_and_parse_args() -> argparse.Namespace:
    """Define and parse the command line arguments for this script."""
    parser = argparse.ArgumentParser(description="Download job results files from IVERT.")
    parser.add_argument("-n", "--job_name", dest="job_name", default=None,
                        help="The name of the job to download results for. Usually in the format 'username_jobid'. Default: Download whatever the latest job was that you submitted from this machine.")
    parser.add_argument("-d" "--dest", dest="dest", default=None,
                        help="The directory to download the results to. Default: Download to the .ivert/jobs sub-directory where the job was submitted.")

    return parser.parse_args()


if __name__ == "__main__":
    args = define_and_parse_args()

    if args.dest is None:
        if args.job_name is None:
            # Look for the "most recent" job in your local .ivert/jobs directory.
            args.dest = find_most_recent_job_dir_from_this_machine()

        else:
            args.dest = find_matching_job_dir(args.job_name)

    if args.job_name is None:
        # Look for the "most recent" job in your local .ivert/jobs directory.
        args.job_name = os.path.basename(find_most_recent_job_dir_from_this_machine())

    download_job(args.job_name, args.dest)
