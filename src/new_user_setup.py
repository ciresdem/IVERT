#!/usr/bin/env python
"""Code for setting up a new IVERT user on the client side (local machine, external to the IVERT cloud)."""

import argparse
import boto3
# import glob
import os
import re
import sys
import shutil
# import textwrap

try:
    import client_job_upload
    from utils.bcolors import bcolors
    import utils.configfile as configfile
    import utils.is_email as is_email
except ModuleNotFoundError:
    # When this is built a setup.py package, it names the module 'ivert'. This reflects that.
    import ivert.client_job_upload as client_job_upload
    from ivert_utils.bcolors import bcolors
    import ivert_utils.configfile as configfile
    import ivert_utils.is_email as is_email

ivert_config = configfile.config()
ivert_user_config_template = configfile.config(ivert_config.ivert_user_config_template)


def setup_new_user(args: argparse.Namespace) -> None:
    """Set up a new IVERT user on the local machine."""

    # First, collect user inputs for any options not provided.
    args = collect_inputs(args, only_if_not_provided=True)

    # Validate all the inputs for basic correctness.
    validate_inputs(args)

    # Confirm the inputs with the user (if requested).
    if args.prompt:
        confirm_inputs_with_user(args)

    # Create the ivert local directories (in ~/.ivert)
    create_local_dirs()

    # Update the AWS profiles on the local machine.
    update_local_aws_profiles(args)

    # Update the IVERT user config file.
    update_ivert_user_config(args)

    # Update the IVERT config file, since the credentials fiels should now be populated.
    global ivert_config
    ivert_config = configfile.config()
    # Gotta do this in the client upload script too, or else these variables won't be there.
    client_job_upload.reset_ivert_config()
    # And the s3 module.

    if args.subscribe_to_sns:
        # Send new_user config (as an "update" command) to the IVERT cloud tool. This will subscribe the user to the IVERT SNS topic.
        subscribe_user_to_sns_notifications(args)

    print("\nIVERT user setup complete!")
    if args.subscribe_to_sns:
        print(f"\nYou sould receive an email from the IVERT server to {bcolors.BOLD}{args.email}{bcolors.ENDC} confirming your subscription to the IVERT SNS notifications.")
        print("\nAfter receiveing that email, you may ", end="")
    else:
        print("\nYou may now ", end="")

    print(f"run\n\n> {bcolors.BOLD}{bcolors.OKBLUE}ivert test{bcolors.ENDC}{bcolors.ENDC}\n\n...to perform a dry run, end-to-end test the IVERT system.\n")
    print(f"At any time, run\n> {bcolors.BOLD}{bcolors.OKBLUE}ivert --help{bcolors.ENDC}{bcolors.ENDC}\n...to see a complete list of other IVERT commands. "
          f"Happy validations!\n")


def read_ivert_s3_credentials(creds_file: str = "", error_if_not_found: bool = True):
    """Read the IVERT S3 credentials file.

    If we're given a path, move it first into the default credentials location."""
    if os.path.exists(creds_file):
        if not os.path.exists(creds_file):
            raise FileNotFoundError(f"IVERT S3 credentials file '{creds_file}' not found.")

        # Move the old creds file to the new location
        print("Moving", os.path.basename(creds_file), "to", os.path.dirname(ivert_config.ivert_s3_credentials_file))
        # Create the directory if it doesn't exist yet.
        if not os.path.exists(os.path.dirname(ivert_config.ivert_s3_credentials_file)):
            os.makedirs(os.path.dirname(ivert_config.ivert_s3_credentials_file))

        shutil.move(creds_file, ivert_config.ivert_s3_credentials_file)

    if os.path.exists(ivert_config.ivert_s3_credentials_file):
        return configfile.config(ivert_config.ivert_s3_credentials_file)
    else:
        if error_if_not_found:
            raise FileNotFoundError(f"IVERT S3 credentials file '{ivert_config.ivert_s3_credentials_file}' not found.")
        else:
            return None


def subscribe_user_to_sns_notifications(args: argparse.Namespace) -> None:
    """Send new_user config (as an "update" command) to the IVERT cloud tool. This will subscribe the user to the IVERT SNS topic."""
    if not args.subscribe_to_sns:
        return

    # Since the IVERT import bucket doesn't have an explicit prefix for "subscribe,", we simply upload it as
    # an "update" command with an additional argument "--subscribe_to_sns"). This isn't in the ivert_client CLI but
    # we fake it here.
    # Create a copy of the original arguments
    args_copy = argparse.Namespace(**vars(args))

    args_copy.command = "update"
    args_copy.sub_command = "subscribe"

    # In the ivert "subscribe" command, the parameter goes by "all", not "filter_sns".
    args_copy.all = not args_copy.filter_sns
    if hasattr(args_copy, "filter_sns"):
        del args_copy.filter_sns
    if hasattr(args_copy, "subscribe_to_sns"):
        del args_copy.subscribe_to_sns
    if hasattr(args_copy, "creds"):
        del args_copy.creds

    # Remove references to bucket names and access keys, as well as the local AWS profile names.
    # They are for setting up the local machine with AWS S3 credentials.
    # The IVERT server doesn't need those and we don't need to transmit them publicly.
    del args_copy.untrusted_bucket_name
    del args_copy.untrusted_access_key_id
    del args_copy.untrusted_secret_access_key
    del args_copy.export_bucket_name
    del args_copy.export_access_key_id
    del args_copy.export_secret_access_key
    del args_copy.ivert_ingest_profile
    del args_copy.ivert_export_profile
    # Can delete username since that will be grabbed from the user config file (that we just set up)
    # in the ivert_client_job_upload:upload_new_job() function
    del args_copy.username
    # The "prompt" argument is not needed for the IVERT server.
    del args_copy.prompt

    # Send the command to the IVERT cloud tool.
    client_job_upload.upload_new_job(args_copy)

    return


def collect_inputs(args: argparse.Namespace, only_if_not_provided: bool = True) -> argparse.Namespace:
    """Collect user inputs and return them as a dictionary.

    Any arguments that were not provided at the command line will be collected from existing config and credentials files."""

    # Check to make sure all the args are present here.
    assert "email" in args
    assert "creds" in args
    assert "username" in args
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

    if not args.username.strip():
        args.username = args.email.split("@")[0]

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
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            print(s3_creds_obj)
            print(dir(s3_creds_obj))
            args.untrusted_bucket_name = s3_creds_obj.s3_untrusted_bucket_name

    if not args.export_bucket_name or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            args.export_bucket_name = s3_creds_obj.s3_export_bucket_name

    if not args.untrusted_access_key_id or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            args.untrusted_access_key_id = s3_creds_obj.s3_untrusted_access_key_id

    if not args.untrusted_secret_access_key or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            args.untrusted_secret_access_key = s3_creds_obj.s3_untrusted_secret_access_key

    if not args.export_access_key_id or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            args.export_access_key_id = s3_creds_obj.s3_export_access_key_id

    if not args.export_secret_access_key or not only_if_not_provided:
        if not s3_creds_obj:
            s3_creds_obj = read_ivert_s3_credentials(args.creds)
        if s3_creds_obj is not None:
            args.export_secret_access_key = s3_creds_obj.s3_export_secret_access_key

    if not (args.untrusted_bucket_name and args.export_bucket_name and args.untrusted_access_key_id and
            args.untrusted_secret_access_key and args.export_access_key_id and args.export_secret_access_key):
        raise ValueError("Error in collecting s3 credentials. Please check for the existence of "
                         f"{ivert_config.ivert_s3_credentials_file} and run setup again. "
                         "If error persists, contact the IVERT developers.")

    return args


def validate_inputs(args: argparse.Namespace) -> None:
    """Validate user inputs for correctness. These should be a completed set of args, not a partial set."""
    assert "email" in args
    assert "username" in args
    assert "creds" in args
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


def create_local_dirs() -> None:
    """Create the local directories needed to store IVERT user data."""
    creds_folder = ivert_config.user_data_creds_directory
    jobs_folder = ivert_config.ivert_jobs_directory_local

    if not os.path.exists(creds_folder):
        os.makedirs(creds_folder)
    if not os.path.exists(jobs_folder):
        os.makedirs(jobs_folder)

    return


def update_ivert_user_config(args: argparse.Namespace) -> None:
    """Create or overwrite the ivert_user_config_[name].ini file."""
    # First, find all instances of existing user config files in the config/ directory.
    user_config_file = ivert_config.user_configfile

    # Get the text from the user config template.
    with open(ivert_config.ivert_user_config_template, "r") as f:
        user_config_text = f.read()

    # Update the user config text with the new values.
    user_config_text = re.sub(r"user_email\s*=\s*[\w\[\]\.@\-]+", f"user_email = {args.email}", user_config_text)


    # Update the username in the user config text.
    user_config_text = re.sub(r"username\s*\=\s*[\w\[\]\.\-]+", f"username = {args.username}", user_config_text)

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
    parser.add_argument("email", type=is_email.return_email,
                        help="The email address of the user.")
    parser.add_argument("-u", "--username", dest="username", type=str, required=False, default="",
                        help="The username of the new user. Only needed if you want to create a custom username. "
                             "Default: Derived from the left side of your email. It's recommended you just "
                             "use the default unless you have specific reasons to do otherwise.")
    parser.add_argument("-c", "--creds", dest="creds", type=str, required=False, default="",
                        help="The path to the 'ivert_s3_credentials.ini' file. This file will be moved to your ~/.ivert/creds directory.")
    parser.add_argument("-ns", "--do_not_subscribe", dest="subscribe_to_sns", action="store_false",
                        default=True, required=False,
                        help="Do not subscribe the new user to the IVERT SNS topic to receive email notifications. "
                             "Default: Subscribe to email notifications. (If you were already subscribed with the "
                             "same email, the subscription settings will just be overwritten. You will not be "
                             "'double-subscribed'.)")
    # Note, this option is an "opt-out", but gets saved as a positive "opt-in" in the variable.
    parser.add_argument("-nf", "--no_sns_filter", dest="filter_sns", action="store_false",
                        default=True, required=False,
                        help="Do not filter email notifications by username. This will make you receive all emails from all jobs. "
                             "Does nothing if the -ns flag is used. "
                             "Default: Only get emails for jobs that *this* user submits.")
    parser.add_argument("-p", "--prompt", dest="prompt", default=False, action="store_true",
                        help="Prompt the user to verify settings before setting up IVERT. Use if you'd like to have "
                             "'one last look' before the setup scripts complete. (If something was wrong, you can "
                             "always re-run this command and it will overwrite old settings. Default: False (no prompt)")

    bucket_group = parser.add_argument_group("IVERT S3 bucket settings",
                              description="Manually enter the IVERT S3 bucket settings and credentials. It is FAR EASIER "
                              "to skip these options, copy the 'ivert_s3_credentials.ini' file from the "
                              "team's GDrive, and place it in ~/.ivert/ivert_s3_credentials.ini. The script will "
                              "automatically grab all these variables from there.")
    bucket_group.add_argument("-ub", "--untrusted_bucket_name", dest="untrusted_bucket_name",
                              default="", type=str, required=False,
                              help="The name of the bucket where untrusted data uploaded to IVERT.")
    bucket_group.add_argument("-uak", "--untrusted_access_key_id", dest="untrusted_access_key_id",
                              default="", type=str, required=False,
                              help="The access key ID for the bucket where untrusted data uploaded to IVERT.")
    bucket_group.add_argument("-usk", "--untrusted_secret_access_key", dest="untrusted_secret_access_key",
                              default="", type=str, required=False,
                              help="The secret access key for the bucket where untrusted data uploaded to IVERT.")
    bucket_group.add_argument("-xb", "--export_bucket_name", dest="export_bucket_name",
                              default="", type=str, required=False,
                              help="The name of the bucket where IVERT data is exported to be downloaded.")
    bucket_group.add_argument("-xak", "--export_access_key_id", dest="export_access_key_id",
                              default="", type=str, required=False,
                              help="The access key ID for the bucket where IVERT data is exported to be downloaded.")
    bucket_group.add_argument("-xsk", "--export_secret_access_key", dest="export_secret_access_key",
                              default="", type=str, required=False,
                              help="The secret access key for the bucket where IVERT data is exported to be downloaded.")

    aws_group = parser.add_argument_group("IVERT AWS profile settings",
                              description="Manually enter the IVERT AWS profile names. Only useful if either of these "
                              "names are already used and you want to avoid conflicts. It's recommended to just use "
                              "the default settings here and skip these options.")
    aws_group.add_argument("-ip", "--ivert_ingest_profile", dest="ivert_ingest_profile",
                           default=ivert_user_config_template.aws_profile_ivert_ingest,
                           type=str, required=False,
                           help="Manually set the name of the AWS profile for IVERT ingest. "
                                f" Default: '{ivert_user_config_template.aws_profile_ivert_ingest}'.")
    aws_group.add_argument("-xp", "--ivert_export_profile", dest="ivert_export_profile",
                           default=ivert_user_config_template.aws_profile_ivert_export,
                           type=str, required=False,
                           help="Manually set the name of the AWS profile for IVERT export. "
                                f"Default: '{ivert_user_config_template.aws_profile_ivert_export}'.")

    if just_return_parser:
        return parser
    else:
        return parser.parse_args()


if __name__ == "__main__":
    input_args = define_and_parse_args()

    # Just for local testing. Normally this is run as a subset menu of ivert_client.py, not standalone. But it can be run standalone.
    setup_new_user(input_args)