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


def run_update_command(args: argparse.Namespace) -> None:
    """Run an update command from the ivert_client to the ivert_server."""

    # Make a copy we can modify to generate a config file for the job.
    args_to_send = argparse.Namespace(**vars(args))
