"""Code for uploading new jobs to the S3 bucket."""

import argparse
import datetime
import glob
import os

import ivert_jobs_database
import s3
import utils.configfile
import utils.progress_bar

# The ivert_config object loads user information from the user's config file if it exists.
ivert_config = utils.configfile.config()


def create_new_job_params(username: str = None, user_email: str = None) -> tuple[str, int, str]:
    """Create a new job number, username, and email address..

    Args:
        username (str, optional): The username of the user. Defaults to just getting it from the config file.
        user_email (str, optional): The email address of the user. Defaults to just getting it from the config file.

    Returns:
        str: Username
        int: New job number
        str: User email address
    """
    # Since this is running on the client, we only get the functionality of the base class, not the server.
    dbo = ivert_jobs_database.IvertJobsDatabaseBaseClass()
    last_job_number = dbo.fetch_latest_job_number_from_s3_metadata()

    if last_job_number is None:
        raise FileNotFoundError(
            "Error connecting to the S3 jobs database. Check your online connection and/or contact your IVERT developers.")

    # Get the user email address.
    if not user_email:
        user_email = ivert_config.user_email

    if not user_email:
        raise FileNotFoundError(
            "Did not get user email from user's config file. Run 'ivert setup' to set up your IVERT account locally.")

    # Get the username.
    if not username:
        username = ivert_config.username

    # If the username isn't set in the config file, just take it from the user's email address.
    if not username:
        username = user_email.split("@")[0]

    # The last job number is YYYYMMDDNNNN. If the last job was "today", we just increment it. Otherwise, we create a new job number using today's date.
    today_str = datetime.date.today().strftime("%Y%m%d")
    last_job_date_str = str(last_job_number)[:8]

    # Create a new job number. If the last job was "today", we just increment it to the next job number from today.
    # Otherwise, we create a new job number using today's date.
    if today_str == last_job_date_str:
        new_job_number = last_job_number + 1
    else:
        new_job_number = int(datetime.date.today().strftime("%Y%m%d")) * 10000

    return username, new_job_number, user_email


def wrap_fname_in_quotes_if_needed(fn: str) -> str:
    """If a filename has spaces, or is empty, wrap it in quotes."""
    return fn if (" " not in f and len(fn) > 0) else f'"{fn}"'


def create_new_job_config(ivert_args: argparse.Namespace,
                          verbose: bool = True) -> tuple[str, str, list[str]]:
    """Create a new job config file to upload to the S3 bucket.

    Saves the config file locally and returns a path to it.

    Args:
        ivert_args (argparse.Namespace): The parsed arguments from the command line given to ivert_client.py.
            See ivert_client.py for listings of these arguments.
        verbose (bool, optional): Whether to print the path to the new job config file. Defaults to True.

    Returns:
        str: The path to the new job config file.
        str: The upload prefix for the new job config file.
        list[str]: The list of other files to upload.
    """
    # Make a copy of the namespace that we can modify for submission.
    args = argparse.Namespace(**vars(ivert_args))
    # Make sure there's a command args in there, and that it's one of the allowed commands.
    assert hasattr(args, "command")
    args.command = args.command.strip().lower()
    assert args.command in ivert_config.ivert_commands

    config_text = grab_job_config_template()
    username, new_job_number, user_email = create_new_job_params(ivert_config.username, ivert_config.user_email)

    # Genereate the full upload prefix for this new job.
    upload_prefix = str(os.path.join(ivert_config.s3_import_prefix_base, args.command, username, str(new_job_number)))
    # AWS S3 paths only use forward-slashes.
    upload_prefix = upload_prefix.replace(os.path.sep, "/")

    job_name = f"{username}_{new_job_number}"

    command = args.command

    files = None
    # Gather the list of files to include in the job config file.
    if hasattr(args, "files"):
        if type(args.files) is str:
            files = [args.files]
        elif type(args.files) in [set, list, tuple]:
            files = args.files
        else:
            raise ValueError(f"Unexpected type of 'files' argument: {type(args.files)}")

        files_out = []
        # Check to see if any files contain a glob pattern. If so, expand it.
        for fn in files:
            if s3.S3Manager.contains_glob_flags(fn):
                matching_fnames = glob.glob(os.path.expanduser(fn))
                files_out.extend(matching_fnames)

        # Check to see if any of the files have spaces in the name. If so, wrap them in quotes.
        files_text = " ".join([wrap_fname_in_quotes_if_needed(os.path.basename(f.strip())) for f in files_out])
        if len(files_text) == 0:
            files_text = '""'

        del args.files

    else:
        files_out = []
        files_text = '""'

    # These variables are stored elsewhere in the config file. Remove them from the namespace of "extra arguments"
    if hasattr(args, "command"):
        del args.command
    if hasattr(args, "username"):
        del args.username
    if hasattr(args, "user_email"):
        del args.user_email
    if hasattr(args, "user"):
        del args.user
    if hasattr(args, "email"):
        del args.email

    # Get the command arguments as a string. Remove the "Namespace(...)" part of the string.
    cmd_args_text = repr(vars(args))
    if len(cmd_args_text) == 0:
        cmd_args_text = '{}'

    # Now that we've gathered all the fields needed, insert them into the config template text.
    config_text = grab_job_config_template()
    config_text = config_text.replace("[USERNAME]", username) \
        .replace("[EMAIL_ADDRESS]", user_email) \
        .replace("[JOB_ID]", str(new_job_number)) \
        .replace("[JOB_NAME]", job_name) \
        .replace("[JOB_UPLOAD_PREFIX]", upload_prefix) \
        .replace("[JOB_COMMAND]", command) \
        .replace("[LIST_OF_FILES]", files_text) \
        .replace("[PARAMS_STRING]", cmd_args_text)

    # Create the new job local directory if it doesn't exist.
    local_jobdir = os.path.join(ivert_config.ivert_jobs_directory_local, job_name)
    if not os.path.exists(local_jobdir):
        os.makedirs(local_jobdir)

    # Write out the new config file.
    new_job_config_fname = os.path.join(local_jobdir, f"{job_name}.ini")
    with open(new_job_config_fname, 'w') as f:
        f.write(config_text)

    if verbose:
        print(f"{new_job_config_fname} created.")

    return new_job_config_fname, upload_prefix, files_out


def grab_job_config_template() -> str:
    job_template_file = ivert_config.ivert_job_config_template
    assert os.path.exists(job_template_file)
    return open(job_template_file, 'r').read()


def upload_new_job(args: argparse.Namespace, verbose=True):
    """Upload a new job to the S3 bucket.

    Will create a local config file and upload it to the S3 bucket along with all the files associated with the job.
    """
    new_job_config_fname, upload_prefix, list_of_other_files = create_new_job_config(args, verbose=verbose)

    N = len(list_of_other_files) + 1
    if verbose:
        print(f"Uploading {N} files to the S3 bucket at {upload_prefix}/")

    # Upload the config file to the S3 bucket.
    s3m = s3.S3Manager()
    s3m.upload(new_job_config_fname,
               upload_prefix + "/" + os.path.basename(new_job_config_fname),
               bucket_type="untrusted")

    if verbose:
        utils.progress_bar.ProgressBar(1, N, decimals=0, suffix=f"1/{N}")

    # Upload the other files to the S3 bucket.
    for i, f in enumerate(list_of_other_files):
        s3m.upload(f,
                   upload_prefix + "/" + os.path.basename(f),
                   bucket_type="untrusted")

        if verbose:
            utils.progress_bar.ProgressBar(2 + i, N, decimals=0, suffix=f"{2 + i}/{N}")

    return


if __name__ == "__main__":
    print(create_new_job_params())
