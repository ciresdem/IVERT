"""Methods for pushing subscribe and unsubscribe commands to the IVERT server."""

import argparse

import ivert_client_job_upload


def run_subscribe_command(args: argparse.Namespace) -> None:
    """From the ivert_client CLI, push a subscribe command to the IVERT server.

    Args:
        args (argparse.Namespace): The parsed arguments from the ivert_client CLI.
    """
    # First, set up the arguments
    args_for_server = argparse.Namespace(**vars(args))  # Create a copy of the original arguments
    args_for_server.command = "update"
    args_for_server.sub_command = "subscribe"

    if args.username is None:
        args_for_server.username = args.email.split("@")[0]

    ivert_client_job_upload.upload_new_job(args_for_server)

    print("\nYour subscribe request has been sent to the IVERT server.")
    print("You should receive an email shortly from Amazon Nofications to confirm your subscription.")
    return


def run_unsubscribe_command(args: argparse.Namespace) -> None:
    """From the ivert_client CLI, push an unsubscribe command to the IVERT server.

    Args:
        args (argparse.Namespace): The parsed arguments from the ivert_client CLI.
    """    # First, set up the arguments
    args_for_server = argparse.Namespace(**vars(args))  # Create a copy of the original arguments
    args_for_server.command = "update"
    args_for_server.sub_command = "unsubscribe"

    args_for_server.username = args.email.split("@")[0]

    ivert_client_job_upload.upload_new_job(args_for_server)

    print("\nYour unsubscribe request has been sent to the IVERT server.")
    print("You will not get a notification back when the job is done (that's kind of the nature of this request, right?).")
    print("You may resubscribe at any time using the 'subscribe' command.")
    return
