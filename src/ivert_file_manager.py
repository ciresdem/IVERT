"""ivert_file_manager.py -- Code for managing cloud files and IVERT instances within the EC2 instance."""

import argparse
import glob
import os
import s3
import subprocess
import sys

import utils.configfile

def import_ivert_input_data(s3_key: str,
                            local_dir: str,
                            s3_bucket_type: str="trusted",
                            create_local_dir: bool=True,
                            verbose: bool=True) -> list:
    """Copies files from an S3 bucket directory to a local directory.

    For a list of s3 IVERT bucket types, see s3.py.

    Args:
        s3_key (str): The key of a directory/prefix, or file, in the S3 bucket.
        local_dir (str): The local directory to copy the file to.
        s3_bucket_type (str, optional): The type of S3 bucket. Defaults to "trusted".
        create_local_dir (bool, optional): Whether to create the local directory if it doesn't exist. Defaults to True.
        verbose (bool, optional): Whether to print verbose output. Defaults to True.

    Returns:
        A list of the files copied. If no files were copied, an empty list is returned.
    """
    # Create the local directory if it doesn't exist.
    if not os.path.exists(local_dir):
        if create_local_dir:
            os.makedirs(local_dir)
        else:
            if verbose:
                print("Error: Local directory does not exist and create_local_dir is False. No files imported.",
                      file=sys.stderr)
            return []
    elif not os.path.isdir(local_dir):
        if verbose:
            print(f"Error: '{local_dir}' already exists and is not a directory.", file=sys.stderr)
        return []

    # At this point it should be both exist and be a directory.
    assert os.path.exists(local_dir) and os.path.isdir(local_dir)

    # Get a list of the files in the S3 bucket
    s3m = s3.S3Manager()
    if s3m.exists(s3_key, bucket_type=s3_bucket_type):
        if s3m.is_existing_s3_directory(s3_key, bucket_type=s3_bucket_type):
            file_list = s3m.listdir(s3_key, bucket_type=s3_bucket_type)
        else:
            file_list = [s3_key]
    else:
        if verbose:
            print(f"Error: S3 key '{s3_key}' does not exist on bucket '{s3m.get_bucketname(bucket_type=s3_bucket_type)}'.",
                  "No files imported.",
                  file=sys.stderr)
        return []

    # Copy the files
    local_files = []
    for s3_file in file_list:
        local_fname = os.path.join(local_dir, s3_file.split("/")[-1])
        s3m.download(s3_file, local_fname, bucket_type=s3_bucket_type, fail_quietly=not verbose)
        if os.path.exists(local_fname):
            local_files.append(local_fname)

    return local_files


def export_ivert_output_data(local_dir_file_or_list, s3_dir, s3_bucket_type="export", file_pattern="*", verbose=True):
    """Copies files from a local directory in the EC2 to an S3 bucket.

    For a list of s3 IVERT bucket types, see s3.py.

    Args:
        local_dir (str): Local directory path where the files are located.
        s3_dir (str): Key to identify the destination prefix in the S3 bucket.
        s3_bucket_type (str, optional): Type of S3 bucket. Defaults to "export".
        file_pattern (str, optional): Pattern to match files. Defaults to "*".
        verbose (bool, optional): Verbosity flag. Defaults to True.

    Returns:
        list: A list of the files keys copied. If no files were copied, an empty list is returned.
    """
    if type(local_dir_file_or_list) in (list, tuple):
        local_files = local_dir_file_or_list

    else:
        local_files = [local_dir_file_or_list]

    files_uploaded = []

    for local_file in local_files:
        # Check if the local directory exists
        if not os.path.exists(local_file):
            if verbose:
                print(f"Error: '{local_file}' does not exist. No files exported.", file=sys.stderr)
            return []

        # List files based on if local_dir is a directory or a single file
        if os.path.isdir(local_file):
            file_list = glob.glob(os.path.join(local_file, file_pattern))
        else:
            file_list = [local_file]

        s3m = s3.S3Manager()

        # Upload the files to S3
        for local_fname in file_list:
            s3_file = "/".join([s3_dir, os.path.basename(local_fname)]).replace("//", "/")
            s3m.upload(local_fname, s3_file, bucket_type=s3_bucket_type, delete_original=False, fail_quietly=not verbose)
            if s3m.exists(s3_file, bucket_type=s3_bucket_type):
                files_uploaded.append(s3_file)
            elif verbose:
                print(f"Error: File '{s3_file}' not uploaded to '{s3m.get_bucketname(bucket_type=s3_bucket_type)}'.",
                      file=sys.stderr)

    return files_uploaded


def clean_up_finished_jobs(verbose=True):
    """Clean up local data files from completed jobs."""
    # TODO: Have this query the database and see what jobs might be still running (vs completed).
    # For now, just clear out the "inputs" and "outputs" directories.
    ivert_config = utils.configfile.config()
    if not ivert_config.is_aws:
        if verbose:
            print("Not in AWS. Not cleaning up finished jobs.")
        return

    rm_inputs_cmd = f"rm -rf {ivert_config.ivert_inputs_directory_local}/*"
    if verbose:
        print(rm_inputs_cmd)
    subprocess.run(rm_inputs_cmd, shell=True)

    rm_outputs_cmd = f"rm -rf {ivert_config.ivert_outputs_directory_local}/*"
    if verbose:
        print(rm_outputs_cmd)
    subprocess.run(rm_outputs_cmd, shell=True)

    return

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


def add_common_args(parser):
    """Parse common command line arguments, used by all sub-commands."""
    parser.add_argument("-q", "--quiet", action="store_true", help="Run silently")
    return parser


def define_and_parse_args():
    """Define and parse command line arguments."""
    ################################################
    # Main parser
    ################################################
    parser = argparse.ArgumentParser(description="A python utility for managing and moving IVERT data files around.")

    subparsers = parser.add_subparsers(dest="command",
                                       help="The command to execute. Options: 'import', 'export', 'clean'. (Only 'clean' is implemented so far.)",
                                       required=True)

    ################################################
    # 'clean' parser
    ################################################
    clean_parser = subparsers.add_parser("clean",
                                         help="Clean up local data files from completed jobs.",
                                         description="Clean up local data files from completed jobs.",
                                         add_help=True)
    clean_parser.add_argument("job_id", default=['all'], type=str,
                              help="The job ID(s) to clean up, in the form <user.name>_<job_id>. "
                                   "Use 'all' to clean up all jobs. (Default: all)",
                              nargs="*")
    add_common_args(clean_parser)

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = define_and_parse_args()
    if args.command == "clean":
        print(args.job_id)
        # clean_up_finished_jobs(verbose=not args.quiet)

    elif args.command == "export":
        # Wait until we have job-tracking implemented.
        raise NotImplementedError(f"Command '{args.command}' not yet implemented.")

    elif args.command == "import":
        # Wait until we have job-tracking implemented.
        raise NotImplementedError(f"Command '{args.command}' not yet implemented.")

    else:
        raise NotImplementedError(f"Uknown command: '{args.command}'")