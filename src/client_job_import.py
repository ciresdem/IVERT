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


def run_import_command(args: argparse.Namespace) -> None:
    """Run an import command from the ivert_client to the ivert_server."""

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
        raise FileNotFoundError(f"File not found: {fn}")

    # Read lists from any text files if "read_as_list" is enabled.from
    if args.read_textfiles:
        files_to_send_2 = []
        for fn in files_to_send:
            if os.path.splitext(fn)[-1].lower() == ".txt":
                with open(fn, 'r') as f:
                    files_to_send_2.extend([fn.strip() for fn in f.readlines() if len(fn.strip()) > 0])
            else:
                files_to_send_2.append(fn)

        files_to_send = files_to_send_2
        del files_to_send_2

    # Strip off client-only arguments
    del args_to_send.files_or_directory
    del args_to_send.prompt
    del args_to_send.read_textfiles

    # Append the "files" argument to the args_to_send object
    args_to_send.files = files_to_send

    # Upload the job
    client_job_upload.upload_new_job(args_to_send)
