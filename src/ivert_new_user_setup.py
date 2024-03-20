"""Code for setting up a new IVERT user on the client side (local machine, external to the IVERT cloud)."""

import argparse
import boto3
import glob
import os
import re
import sys
import textwrap

import utils.configfile
from utils.bcolors import bcolors

ivert_config = utils.configfile.config()
ivert_user_config_template = utils.configfile.config(ivert_config.ivert_user_config_template)

def setup_new_user(args: argparse.Namespace) -> None:
    """Set up a new IVERT user on the local machine."""

    # TODO: Modify code to include s3 buckets in user profile.
    print("TODO: MODIFY TO INCLUDE S3 BUCKETS IN USER PROFILE.")
    sys.exit(0)

    # First, collect user inputs for any options not provided.
    args = collect_user_inputs(args, only_if_not_provided=True)

    # Validate all the inputs for basic correctness.
    validate_inputs(args)

    # Confirm the inputs with the user.
    confirm_inputs_with_user(args)

    # Update the AWS profiles on the local machine.
    update_local_aws_profiles(args)

    # Update the IVERT user config file.
    update_ivert_user_config(args)

    print("\nIVERT user setup complete!")
    print(f"\nYou may now run '{bcolors.BOLD}python ivert.py test{bcolors.ENDC}' to test the IVERT system.")


def collect_user_inputs(args: argparse.Namespace, only_if_not_provided: bool = True) -> argparse.Namespace:
    """Collect user inputs and return them as a dictionary."""
    print("Please enter the following credentials to set up your IVERT account.")
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

    DEBUG = False

    if DEBUG:
        args.email = "michael.macferrin@noaa.gov"
        args.untrusted_bucket_name = "nccf-dev-testing-nonsense-us-east-1-12345678910"
        args.untrusted_access_key_id = "NONSENSE20CHARS00000"
        args.untrusted_secret_access_key = "Nonsense40CharactersForTesting/foobar+baz"
        args.export_bucket_name = "nccf-ssbx-testing-nonsense-us-east-1-12345678910"
        args.export_access_key_id = "ANOTHERSILLY20CHARS0"
        args.export_secret_access_key = "Nonsense40CharactersForTesting/baz+foobar"

    else:
        if not args.email.strip() or not only_if_not_provided:
            args.email = input("\n" + bcolors.UNDERLINE + "Your email address" + bcolors.ENDC + " (@noaa.gov): ").strip()

        # Check for valid AWS profile names (they can basically be anyting except empty strings)
        if not args.ivert_ingest_profile.strip() or not only_if_not_provided:
            args.ivert_ingest_profile = (
              input(f"\nAWS profile name to use for ingest [default: {ivert_user_config_template.aws_profile_ivert_ingest}]: ").strip()
              or ivert_user_config_template.aws_profile_ivert_ingest.strip())

        if not args.ivert_export_profile.strip() or not only_if_not_provided:
            args.ivert_export_profile = (
              input(f"\nAWS profile name to use for export [default: {ivert_user_config_template.aws_profile_ivert_export}]: ").strip()
              or ivert_user_config_template.aws_profile_ivert_export.strip())

        credentials_msg = "\n".join(textwrap.wrap("Note: The following credentials can be retrieved from the team's"
                          " 'IVERT Bucket IAM Credentials' document. If you don't have access to this document, please"
                          " contact the IVERT team for more information. [Ctrl-C] to exit at any time.",
                                                   os.get_terminal_size().columns - 1))
        credentials_msg = credentials_msg.replace("Note:", "\n" + bcolors.HEADER + "Note:" + bcolors.ENDC)
        credentials_msg += "\n"
        already_printed_credentials_msg = False

        # Strip any whitespace
        args.untrusted_bucket_name = args.untrusted_bucket_name.strip()
        args.export_bucket_name = args.export_bucket_name.strip()
        args.untrusted_access_key_id = args.untrusted_access_key_id.strip()
        args.untrusted_secret_access_key = args.untrusted_secret_access_key.strip()
        args.export_access_key_id = args.export_access_key_id.strip()
        args.export_secret_access_key = args.export_secret_access_key.strip()

        try:
            if not args.untrusted_bucket_name or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.untrusted_bucket_name = input(bcolors.UNDERLINE + "Untrusted bucket name" + bcolors.ENDC + ": ").strip().lstrip("s3://")

            if not args.untrusted_access_key_id or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.untrusted_access_key_id = input(bcolors.UNDERLINE + "Untrusted access key ID" + bcolors.ENDC + ": ").strip()

            if not args.untrusted_secret_access_key or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.untrusted_secret_access_key = input(bcolors.UNDERLINE + "Untrusted secret access key" + bcolors.ENDC + ": ").strip()

            if not args.export_bucket_name or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.export_bucket_name = input(bcolors.UNDERLINE + "Export bucket name" + bcolors.ENDC + ": ").strip().lstrip("s3://")

            if not args.export_access_key_id or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.export_access_key_id = input(bcolors.UNDERLINE + "Export access key ID" + bcolors.ENDC + ": ").strip()

            if not args.export_secret_access_key or not only_if_not_provided:
                if not already_printed_credentials_msg:
                    print(credentials_msg)
                    already_printed_credentials_msg = True
                args.export_secret_access_key = input(bcolors.UNDERLINE + "Export secret access key" + bcolors.ENDC + ": ").strip()

        except KeyboardInterrupt:
            print()
            sys.exit(1)

    # Derive the user name from the email
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

    # Get max lengths of each field:
    max_left = max([len(k) for k in ("Email",
                                     "Username",
                                     "Untrusted bucket",
                                     "Untrusted access key ID",
                                     "Untrusted secret access key",
                                     "Export bucket",
                                     "Export access key ID",
                                     "Export secret access key",)])
    for k, v in zip(("Email",
                     "Username",
                     "Untrusted bucket",
                     "Untrusted access key ID",
                     "Untrusted secret access key",
                     "Export bucket",
                     "Export access key ID",
                     "Export secret access key",),
                    (args.email,
                     args.user,
                     args.untrusted_bucket_name,
                     args.untrusted_access_key_id,
                     args.untrusted_secret_access_key,
                     args.export_bucket_name,
                     args.export_access_key_id,
                     args.export_secret_access_key,)):
        print(f"  {(k + r": ").ljust(max_left + 2)}{v}")

    print()
    print(bcolors.OKGREEN + bcolors.BOLD + "Is this correct?" + bcolors.ENDC + bcolors.ENDC, end="")
    params_confirmed = (input(" [y]/n:") or "y")

    if params_confirmed[0].lower() != "y":
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

    # print("\nOld AWS Config file contents:")
    # print(config_text_old)
    #
    # print("\nNew AWS Config file contents:")
    # print(config_text)
    # return

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

def find_user_config_files() -> list[str]:
    """Find all user config files in the config/ directory."""
    user_config_files = glob.glob(os.path.join(os.path.dirname(ivert_config.ivert_user_config_template),
                                  ivert_config.ivert_user_config_wildcard))

    # Ignore any files that are the template file.
    user_config_files = [f for f in user_config_files if os.path.basename(f) != os.path.basename(ivert_config.ivert_user_config_template)]

    return user_config_files

def update_ivert_user_config(args: argparse.Namespace) -> None:
    """Create or overwrite the ivert_user_config_[name].ini file."""
    # First, find all instances of existing user config files in the config/ directory.
    user_config_files = find_user_config_files()

    # Get the text from the user config template.
    with open(ivert_config.ivert_user_config_template, "r") as f:
        user_config_text = f.read()

    # Update the user config text with the new values.
    # Update the email in the user config text.
    user_config_text = re.sub(r"\[EMAIL_ADDRESS\]", args.email, user_config_text)

    # Update the username in the user config text.
    user_config_text = re.sub(r"\[USERNAME\]", args.user, user_config_text)

    # Update the aws_profile_ivert_ingest in the user config text, if needed.
    # If it's using a different profile name, then update it.
    if args.ivert_ingest_profile != re.search(r"(?<=aws_profile_ivert_ingest)(\s*\=\s*\w+)", user_config_text).group(1).lstrip().lstrip("=").lstrip():
        user_config_text = re.sub(r"aws_profile_ivert_ingest\s*\=\s*\w+", f"aws_profile_ivert_ingest = {args.ivert_ingest_profile}", user_config_text)

    if args.ivert_export_profile != re.search(r"(?<=aws_profile_ivert_export)(\s*\=\s*\w+)", user_config_text).group(1).lstrip().lstrip("=").lstrip():
        user_config_text = re.sub(r"aws_profile_ivert_export\s*\=\s*\w+", f"aws_profile_ivert_export = {args.ivert_export_profile}", user_config_text)

    # Delete any existing user config files.
    for user_config_file in user_config_files:
        print(f"Removing old IVERT/config/{os.path.basename(user_config_file)}.")
        os.remove(user_config_file)

    # Create the new user config file.
    user_configfile_name = os.path.join(os.path.dirname(ivert_config.ivert_user_config_template),
                                        ivert_config.ivert_user_config_wildcard.replace("*", args.user))

    with open(user_configfile_name, "w") as f:
        f.write(user_config_text)

    print(f"Created IVERT/config/{os.path.basename(user_configfile_name)}.")

    return

def get_aws_config_and_credentials_files() -> list:
    """Find the locations of the AWS config and credentials files.

    Create the config and credentials files if they don't exist."""
    # First, check to see if these locations are set in the environment variables.
    # Documentation from https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html
    config_file = os.environ.get("AWS_CONFIG_FILE")
    credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")

    if config_file is None:
        config_file = os.path.expanduser("~/.aws/config")
    if credentials_file is None:
        credentials_file = os.path.expanduser("~/.aws/credentials")

    return [config_file, credentials_file]

def get_region_name_from_bucket_name(bucket_name: str) -> str:
    """Get the region name from a bucket name."""
    region_names = boto3.Session().get_available_regions("s3")
    for region_name in region_names:
        if region_name in bucket_name:
            return region_name

    raise ValueError(f"Could not find a region name for bucket {bucket_name}.")


def define_and_parse_args(just_return_parser: bool=False):
    """Define and parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Set up a new IVERT user on the local machine.")
    parser.add_argument( "-e", "--email", dest="email", type=str, required=False, default="",
                        help="The email address of the new user. (For now, should be firstname.lastname@noaa.gov.)")
    parser.add_argument( "-u", "--untrusted_bucket_name", dest="untrusted_bucket_name",
                        default="", type=str, required=False,
                        help="The name of the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument("-uak", "--untrusted_access_key_id", dest="untrusted_access_key_id",
                        default="", type=str, required=False,
                        help="The access key ID for the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument( "-usk", "--untrusted_secret_access_key", dest="untrusted_secret_access_key",
                        default="", type=str, required=False,
                        help="The secret access key for the bucket where untrusted data uploaded to IVERT.")
    parser.add_argument( "-xb", "--export_bucket_name", dest="export_bucket_name",
                        default="", type=str, required=False,
                        help="The name of the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument( "-xak", "--export_access_key_id", dest="export_access_key_id",
                        default="", type=str, required=False,
                        help="The access key ID for the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument( "-xsk", "--export_secret_access_key", dest="export_secret_access_key",
                        default="", type=str, required=False,
                        help="The secret access key for the bucket where IVERT data is exported to be downloaded.")
    parser.add_argument( "-ip", "--ivert_ingest_profile", dest="ivert_ingest_profile",
                        default=ivert_user_config_template.aws_profile_ivert_ingest,
                        type=str, required=False,
                        help=f"The name of the AWS profile for IVERT ingest. Default: '{ivert_user_config_template.aws_profile_ivert_ingest}'.")
    parser.add_argument( "-xp", "--ivert_export_profile", dest="ivert_export_profile",
                        default=ivert_user_config_template.aws_profile_ivert_export,
                        type=str, required=False,
                        help=f"The name of the AWS profile for IVERT export. Default: '{ivert_user_config_template.aws_profile_ivert_export}'.")

    if just_return_parser:
        return parser
    else:
        return parser.parse_args()

if __name__ == "__main__":
    input_args = define_and_parse_args()

    # Just for local testing
    setup_new_user(input_args)