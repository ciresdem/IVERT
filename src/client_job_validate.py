"""Code for pre-processing and submitting a validate command from the ivert_client to the ivert_server."""

import argparse
import glob
import os
import sys

if vars(sys.modules[__name__])['__package__'] == 'ivert':
    # When this is built a setup.py package, it names the modules 'ivert' and 'ivert_utils'. This reflects that.
    import ivert.client_job_upload as client_job_upload
    import ivert.s3 as s3
else:
    # If running as a script, import this way.
    import client_job_upload
    import s3


def run_validate_command(args: argparse.Namespace,
                         wait_interval_s: int = 5) -> None:
    """Run a validate command from the ivert_client to the ivert_server."""
    assert hasattr(args, "command") and args.command == "validate"
    assert hasattr(args, "files_or_directory")

    # Make a copy we can modify to generate a config file for the job.
    args_to_send = argparse.Namespace(**vars(args))

    # Run through the files, populate any glob patterns.
    files_to_send = []
    for fn in args.files_or_directory:
        if os.path.isdir(fn):
            files_to_send.extend(glob.glob(os.path.join(fn, "*.tif")))
        elif s3.S3Manager.contains_glob_flags(fn):
            files_to_send.extend(glob.glob(fn))
        else:
            if not os.path.exists(fn):
                raise FileNotFoundError(f"File not found: {fn}")
            files_to_send.append(fn)

    if len(files_to_send) == 0:
        raise ValueError("No files to validate.")

    # Convert to absolute paths.
    files_to_send = [os.path.abspath(fn) for fn in files_to_send]

    # Replace the 'files_or_directory' with the 'files' argument.
    del args_to_send.files_or_directory
    args_to_send.files = files_to_send

    client_job_upload.upload_new_job(args, verbose=True)