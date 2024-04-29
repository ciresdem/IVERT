#!/usr/bin/env python
"""Code for setting up a new IVERT user on the client side (local machine, external to the IVERT cloud)."""

import argparse
import boto3
# import glob
import os
import re
import sys
# import textwrap

import ivert_client_job_upload
from utils.bcolors import bcolors
import utils.configfile
import utils.is_email

ivert_config = utils.configfile.config()
ivert_user_config_template = utils.configfile.config(ivert_config.ivert_user_config_template)


def setup_new_user(args: argparse.Namespace) -> None:
    """Set up a new IVERT user on the local machine."""

    # First, collect user inputs for any options not provided.
    args = collect_inputs(args, only_if_not_provided=True)

    # Validate all the inputs for basic correctness.
    validate_inputs(args)

    # Confirm the inputs with the user.
    # confirm_inputs_with_user(args)

    # Update the AWS profiles on the local machine.
    update_local_aws_profiles(args)

    # Update the IVERT user config file.
    update_ivert_user_config(args)

    # Prompt the user if requested.
    if args.prompt:
        confirm_inputs_with_user(args)

    # Send new_user config (as an "update" command) to the IVERT cloud tool. This will subscribe the user to the IVERT SNS topic.
    send_new_user_config_to_ivert_cloud(args)

    print("\nIVERT user setup complete!")
    print(f"\nYou may now run '{bcolors.BOLD}python ivert.py test{bcolors.ENDC}' to test the IVERT system.")


def read_ivert_s3_credentials(error_if_not_found: bool = True):
    """Read the IVERT S3 credentials file."""
    if os.path.exists(ivert_config.ivert_s3_credentials_file):
        return utils.configfile.config(ivert_config.ivert_s3_credentials_file)
    else:
        if error_if_not_found:
            raise FileNotFoundError(f"IVERT S3 credentials file '{ivert_config.ivert_s3_credentials_file}' not found.")
        else:
            return None


def send_new_user_config_to_ivert_cloud(args: argparse.Namespace) -> None:
    """Send new_user config (as an "update" command) to the IVERT cloud tool. This will subscribe the user to the IVERT SNS topic."""
    if not args.subscribe_to_sns:
        return

    # Since the IVERT import bucket doesn't have an explicit prefix for "subscribe,", we simply upload it as
    # an "update" command with an additional argument "--subscribe_to_sns"). This isn't in the ivert_client CLI but
    # we fake it here.
    # Create a copy of the original arguments
    args_copy = argparse.Namespace(**vars(args))
    args_copy.command = "update"
    args_copy.ADD_NEW_USER = True

    # Remove references to bucket names and access keys, as well as the local AWS profile names.
    # The IVERT server doesn't need those and we don't need to transmit them publicly.
    del args_copy.untrusted_bucket_name
    del args_copy.untrusted_access_key_id
    del args_copy.untrusted_secret_access_key
    del args_copy.export_bucket_name
    del args_copy.export_access_key_id
    del args_copy.export_secret_access_key
    del args_copy.ivert_ingest_profile
    del args_copy.ivert_export_profile
    # Can delete username and email since those will be grabbed from the user config file (that we just set up)
    del args_copy.user
    del args_copy.email
    # The "prompt" argument is not needed for the IVERT server.
    del args_copy.prompt

    # Send the command to the IVERT cloud tool.
    ivert_client_job_upload.upload_new_job(args_copy)


def collect_inputs(args: argparse.Namespace, only_if_not_provided: bool = True) -> argparse.Namespace:
    """Collect user inputs and return them as a dictionary.

    Any arguments that were not provided at the command line will be collected from existing config and credentials files."""

    # Check to make sure all the args are present here.
    assert "email" in args
    assert "untrusted_bucket_name" in args
    assert "untrusted_access_key_id" in args
    assert "untrusted_secret_access_key" in args
    assert "export_bucket_name" in args
    assert "export_access_key_id" in args
    assert "export_secret_access_key" in args
    assert "ivert_ingest_profile" in args
    assert "ivert_export_profile" in args

    if not args.email.strip() or not only_if_not_provided:
        args.email = input("\n" + bcolors.UNDERLINE + "Your email address" + bcolors.ENDC + ": ").strip()

    # Check for valid AWS profile names (they can basically be anyting except empty strings)
    # If we weren't provided a profile name or we aren't using the defaults, prompt for them.
    if not args.ivert_ingest_profile.strip() or not only_if_not_provided:
        args.ivert_ingest_profile = (
          input(f"\nAWS profile name to use for ingest [default: {ivert_user_config_template.aws_profile_ivert_ingest}]: ").strip()
          or ivert_user_config_template.aws_profile_ivert_ingest.strip())

    if not args.ivert_export_profile.strip() or not only_if_not_provided:
        args.ivert_export_profile = (
          input(f"\nAWS profile name to use for export [default: {ivert_user_config_template.aws_profile_ivert_export}]: ").strip()
          or ivert_user_config_template.aws_profile_ivert_export.strip())

    # credentials_msg = "\n".join(textwrap.wrap("Note: The following credentials can be retrieved from the team's"
    #                   " 'IVERT Bucket IAM Credentials' document. If you don't have access to this document, please"
    #                   " contact the IVERT team for more information. [Ctrl-C] to exit at any time.",
    #                                            os.get_terminal_size().columns - 1))
    # credentials_msg = credentials_msg.replace("Note:", "\n" + bcolors.HEADER + "Note:" + bcolors.ENDC)
    # credentials_msg += "\n"
    # already_printed_credentials_msg = False

    # Strip any whitespace
    args.untrusted_bucket_name = args.untrusted_bucket_name.strip()
    args.export_bucket_name = args.export_bucket_name.strip()
    args.untrusted_access_key_id = args.untrusted_access_key_id.strip()
    args.untrusted_secret_access_key = args.untrusted_secret_access_key.strip()
    args.export_access_key_id = args.export_access_key_id.strip()
    args.export_secret_access_key = args.export_secret_access_key.strip()

    # Boolean flags should be strictly True/False
    args.subscribe_to_sns = bool(args.subscribe_to_sns)
    args.filter_sns = bool(args.filter_sns)

    s3_creds_obj = None

    if not args.untrusted_bucket_name or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.untrusted_bucket_name = s3_creds_obj.s3_bucket_import_untrusted

    if not args.export_bucket_name or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.export_bucket_name = s3_creds_obj.s3_bucket_export

    if not args.untrusted_access_key_id or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.untrusted_access_key_id = s3_creds_obj.s3_untrusted_access_key_id

    if not args.untrusted_secret_access_key or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.untrusted_secret_access_key = s3_creds_obj.s3_untrusted_secret_access_key

    if not args.export_access_key_id or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.export_access_key_id = s3_creds_obj.s3_export_access_key_id

    if not args.export_secret_access_key or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials()
        if s3_creds_obj is not None:
            args.export_secret_access_key = s3_creds_obj.s3_export_secret_access_key

    if not (args.untrusted_bucket_name and args.export_bucket_name and args.untrusted_access_key_id and
            args.untrusted_secret_access_key and args.export_access_key_id and args.export_secret_access_key):
        raise ValueError("Error in collecting s3 credentials. Please check for the existence of "
                         f"{ivert_config.ivert_s3_credentials_file} and run setup again. "
                         "If error persists, contact the IVERT developers.")

    # Derive the username from the email
    args.user = args.email.split("@")[0].strip().lower()

    return args


def validate_inputs(args: argparse.Namespace) -> None:
    """Validate user inputs for correctness. These should be a completed set of args, not a partial set."""
    assert "email" in args
    assert "untrusted_bucket_name" in args
    assert "untrusted_access_key_id" in args
    assert "untrusted_secret_access_key" in args
    assert "export_bucket_name" in args
    assert "export_access_key_id" in args
    assert "export_secret_access_key" in args
    assert "ivert_ingest_profile" in args
    assert "ivert_export_profile" in args
    assert "subscribe_to_sns" in args
    assert "filter_sns" in args

    # Validate a correct email address.
    email_regex = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
    if not email_regex.match(args.email):
        print()
        raise ValueError(f"{args.email} is an invalid email address.")
    # Check (for now) specifically for a noaa.gov email.
    if args.email.split("@")[1].lower().strip() != "noaa.gov":
        print()
        raise ValueError(f"Email must be from noaa.gov. '{args.email}' is not from noaa.gov.")

    # Check bucket names for validity.
    # Bucket naming rules from https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    bucket_name_regex = re.compile(
        r'(?!(^((2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})\.){3}(2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})$|^xn--|.+-s3alias$))^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')

    for bname in [args.untrusted_bucket_name, args.export_bucket_name]:
        if not bucket_name_regex.match(bname):
            print()
            raise ValueError(f"{bname} is an invalid bucket name.")

    # Check access key IDs for validity.
    # Access key ID rules from https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
    access_key_id_regex = re.compile(r'(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])')

    for akid in [args.untrusted_access_key_id, args.export_access_key_id]:
        if not access_key_id_regex.match(akid):
            print()
            raise ValueError(f"{akid} is an invalid access key ID.")

    # Check secret access keys for validity.
    # Secret access key rules from https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
    secret_access_key_regex = re.compile(r'(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])')

    for sak in [args.untrusted_secret_access_key, args.export_secret_access_key]:
        if not secret_access_key_regex.match(sak):
            print()
            raise ValueError(f"{sak} is an invalid secret access key.")

    return


def confirm_inputs_with_user(args: argparse.Namespace) -> None:
    """
    Prompt the user to confirm the inputs.
    """
    print()
    print(bcolors.OKGREEN + "Please confirm the following:" + bcolors.ENDC)

    headings = ("Email",
                "Username",
                "Untrusted bucket",
                "Untrusted access key ID",
                "Untrusted secret access key",
                "Export bucket",
                "Export access key ID",
                "Export secret access key",
                "Subscribe to email notifications",
                "Filter email notifications by user",)

    # Get max lengths of each field:
    max_left = max([len(k) for k in headings])
    for k, v in zip(headings,
                    (args.email,
                     args.user,
                     args.untrusted_bucket_name,
                     args.untrusted_access_key_id,
                     args.untrusted_secret_access_key,
                     args.export_bucket_name,
                     args.export_access_key_id,
                     args.export_secret_access_key,
                     args.subscribe_to_sns,
                     args.filter_sns,)):
        print(f"  {(k + r': ').ljust(max_left + 2)}{v}")

    print()
    print(bcolors.OKGREEN + bcolors.BOLD + "Is this correct?" + bcolors.ENDC + bcolors.ENDC, end="")
    params_confirmed = (input(" [y]/n: ") or "y")

    if params_confirmed.strip()[0].lower() != "y":
        print()
        print(bcolors.FAIL + "Exiting." + bcolors.ENDC)
        sys.exit(1)

    print()
    return


def update_local_aws_profiles(args: argparse.Namespace) -> None:
    """
    Update the AWS profiles.
    """
    aws_config_file, aws_credentials_file = get_aws_config_and_credentials_files()
    update_local_aws_config(aws_config_file, args)
    update_local_aws_credentials(aws_credentials_file, args)
    return


def update_local_aws_config(aws_config_file: str,
                            args: argparse.Namespace) -> None:
    """
    Update the AWS config file.
    """
    # if the config file doesn't exist, create it
    if not os.path.exists(aws_config_file):
        with open(aws_config_file, "w") as f:
            f.write("")

    # Check that the config file exists
    assert os.path.exists(aws_config_file)

    # Read the config file.
    with open(aws_config_file, "r") as f:
        config_text = f.read()

    config_text_old = config_text

    # Find eac profile if it already exists. If so, replace it. If not, add it.
    for profile_id_string, bname in [(args.ivert_ingest_profile, args.untrusted_bucket_name),
                                     (args.ivert_export_profile, args.export_bucket_name)]:
        ivert_profile_string = f"[profile {profile_id_string}]"
        new_ivert_profile = "\n".join([ivert_profile_string,
                                       "output = json",
                                       f"region = {get_region_name_from_bucket_name(bname)}"]) + "\n\n"

        if ivert_profile_string in config_text:
            # Try to find the entire profile string with all options, until either the end of the file or the next profile string.
            ivert_profile_search_regex = ivert_profile_string.replace("[", r"\[").replace("]", r"\]") + \
                r"[\w\s\d\=\-\"\']*(?=(\[profile )|\Z)"

            m = re.search(ivert_profile_search_regex, config_text)
        else:
            ivert_profile_search_regex = None
            m = None

        # If we found the profile, replace it. If not, add it.
        if m is None:
            config_text = config_text.rstrip() + "\n\n" + new_ivert_profile
        else:
            config_text = re.sub(ivert_profile_search_regex, new_ivert_profile, config_text, count=1)

    # Overwrite the config file.
    with open(aws_config_file, "w") as f:
        f.write(config_text)

    print(f"Updated {aws_config_file.replace(os.environ['HOME'], '~')}.")

    return


def update_local_aws_credentials(aws_credentials_file: str,
                                 args: argparse.Namespace) -> None:
    """
    Update the AWS credentials file.
    """
    # If it doesn't exist, create it.
    if not os.path.exists(aws_credentials_file):
        with open(aws_credentials_file, "w") as f:
            f.write("")

    # Check that the credentials file exists
    assert os.path.exists(aws_credentials_file)

    # Read the credentials file.
    with open(aws_credentials_file, "r") as f:
        credentials_text = f.read()

    credentials_text_old = credentials_text

    # Find eac profile if it already exists. If so, replace it. If not, add it.
    for profile_id_string, access_key_id, secret_access_key in \
            [(args.ivert_ingest_profile, args.untrusted_access_key_id, args.untrusted_secret_access_key),
             (args.ivert_export_profile, args.export_access_key_id, args.export_secret_access_key)]:
        ivert_profile_string = f"[{profile_id_string}]"
        new_ivert_profile = "\n".join([ivert_profile_string,
                                       f"aws_access_key_id = {access_key_id}",
                                       f"aws_secret_access_key = {secret_access_key}"]) + "\n\n"

        if ivert_profile_string in credentials_text:
            # Try to find the entire profile string with all options, until either the end of the file or the next profile string.
            ivert_profile_search_regex = ivert_profile_string.replace("[", r"\[").replace("]", r"\]") + \
                r"[\w\s\d\=\-\"\'\+/]*(?=(\[[a-zA-Z0-9_]+\])|\Z)"

            m = re.search(ivert_profile_search_regex, credentials_text)
        else:
            ivert_profile_search_regex = None
            m = None

        # If we found the profile, replace it. If not, add it.
        if m is None:
            credentials_text = credentials_text.rstrip() + "\n\n" + new_ivert_profile
        else:
            credentials_text = re.sub(ivert_profile_search_regex, new_ivert_profile, credentials_text, count=1)

    # Overwrite the credentials file.
    with open(aws_credentials_file, "w") as f:
        f.write(credentials_text)

    print(f"Updated {aws_credentials_file.replace(os.environ['HOME'], '~')}.")

    return


def update_ivert_user_config(args: argparse.Namespace) -> None:
    """Create or overwrite the ivert_user_config_[name].ini file."""
    # First, find all instances of existing user config files in the config/ directory.
    user_config_file = ivert_config.user_configfile

    # Get the text from the user config template.
    with open(ivert_config.ivert_user_config_template, "r") as f:
        user_config_text = f.read()

    # Update the user config text with the new values.
    # Update the email in the user config text.
    user_config_text = re.sub(r"user_email\s*\=\s*[\w\[\]\.\@]+", f"user_email = {args.email}", user_config_text)

    # Update the username in the user config text.
    user_config_text = re.sub(r"username\s*\=\s*[\w\[\]\.\-]+", f"username = {args.user}", user_config_text)

    # Update the aws_profile_ivert_ingest in the user config text, if needed.
    # If it's using a different profile name, then update it.
    user_config_text = re.sub(r"aws_profile_ivert_ingest\s*\=\s*[\w\[\]\.\-]+", f"aws_profile_ivert_ingest = {args.ivert_ingest_profile}", user_config_text)
    user_config_text = re.sub(r"aws_profile_ivert_export\s*\=\s*[\w\[\]\.\-]+", f"aws_profile_ivert_export = {args.ivert_export_profile}", user_config_text)

    # Write the boolean flags for the user config file. Overwrite any old ones.
    user_config_text = re.sub(r"subscribe_to_sns\s*\=\s*[\w\[\]\.\-]+", f"subscribe_to_sns = {str(args.subscribe_to_sns)}", user_config_text)
    user_config_text = re.sub(r"filter_sns_by_username\s*\=\s*[\w\[\]\.\-]+", f"filter_sns_by_username = {str(args.filter_sns)}", user_config_text)

    # Write the user config file. Overwrite any old one.
    with open(user_config_file, "w") as f:
        f.write(user_config_text)

    print(f"{user_config_file} written.")

    return


def get_aws_config_and_credentials_files() -> list:
    """Find the locations of the AWS config and credentials files.

    Create the config and credentials files if they don't exist."""
    # First, check to see if these locations are set in the environment variables.
    # Documentation from https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html
    config_file = os.environ.get("AWS_CONFIG_FILE")
    credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")

    # If they aren't in the user environment variables, then use the default locations.
    if config_file is None:
        config_file = os.path.expanduser("~/.aws/config")
    if credentials_file is None:
        credentials_file = os.path.expanduser("~/.aws/credentials")

    return [config_file, credentials_file]


def get_region_name_from_bucket_name(bucket_name: str) -> str:
    """Get the region name from a bucket name. The NCCF bucket names include the region name in them."""
    region_names = boto3.Session().get_available_regions("s3")
    for region_name in region_names:
        if region_name in bucket_name:
            return region_name

    raise ValueError(f"Could not extract a region name from bucket {bucket_name}.")


def define_and_parse_args(just_return_parser: bool=False):
    """Define and parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Set up a new IVERT user on the local machine.")
    parser.add_argument("-e", "--email", dest="email", type=utils.is_email.return_email,
                        required=False, default="",
                        help="The email address of the new user.")
    parser.add_argument("-u", "--untrusted_bucket_name", dest="untrusted_bucket_name",
                        default="", type=str, required=False,
                        help="The name of the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument("-uak", "--untrusted_access_key_id", dest="untrusted_access_key_id",
                        default="", type=str, required=False,
                        help="The access key ID for the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument("-usk", "--untrusted_secret_access_key", dest="untrusted_secret_access_key",
                        default="", type=str, required=False,
                        help="The secret access key for the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument("-xb", "--export_bucket_name", dest="export_bucket_name",
                        default="", type=str, required=False,
                        help="The name of the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument("-xak", "--export_access_key_id", dest="export_access_key_id",
                        default="", type=str, required=False,
                        help="The access key ID for the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument("-xsk", "--export_secret_access_key", dest="export_secret_access_key",
                        default="", type=str, required=False,
                        help="The secret access key for the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument("-ns", "--do_not_subscribe", dest="subscribe_to_sns", action="store_false",
                        default=True, required=False,
                        help="Do not subscribe the new user to the IVERT SNS topic to receive email notifications. "
                             " This is a good option to use if you've already susbcribed to email notifications perviously and don't want to again."
                             " Default: Subscribe to email notifications.")
    parser.add_argument("-nf", "--no_sns_filter", dest="filter_sns", action="store_false",
                        default=True, required=False,
                        help="Do not filter email notifications by username. This will make you receive all emails from all jobs."
                             " Does nothing if the -ns flag is used."
                             " Default: Only get emails that *this* user submits.")
    parser.add_argument("-ip", "--ivert_ingest_profile", dest="ivert_ingest_profile",
                        default=ivert_user_config_template.aws_profile_ivert_ingest,
                        type=str, required=False,
                        help="Manually set the name of the AWS profile for IVERT ingest. "
                             f" Default: '{ivert_user_config_template.aws_profile_ivert_ingest}'.")
    parser.add_argument("-xp", "--ivert_export_profile", dest="ivert_export_profile",
                        default=ivert_user_config_template.aws_profile_ivert_export,
                        type=str, required=False,
                        help="Manually set the name of the AWS profile for IVERT export. "
                             f"Default: '{ivert_user_config_template.aws_profile_ivert_export}'.")
    parser.add_argument("-p", "--prompt", dest="prompt", default=False, action="store_true",
                        help="Prompt the user to verify settings before uploading files to IVERT. Default: False")

    if just_return_parser:
        return parser
    else:
        return parser.parse_args()


if __name__ == "__main__":
    input_args = define_and_parse_args()

    # Just for local testing. Normally this is run as a subset menu of ivert_client.py, not standalone. But it can be run standalone.
    setup_new_user(input_args)