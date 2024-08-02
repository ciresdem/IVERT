import boto3
import os
import sys
from packaging.version import Version

if vars(sys.modules[__name__])['__package__'] == 'ivert_utils':
    # When this is built a setup.py package, it names the modules 'ivert' and 'ivert_utils'. This reflects that.
    import ivert_utils.is_aws as is_aws
    import ivert_utils.configfile as configfile
else:
    try:
        # If running as a script, import this way.
        import is_aws
        import configfile
    except ModuleNotFoundError:
        # If this script is imported from another module in the src/ directory, import this way.
        import utils.is_aws as is_aws
        import utils.configfile as configfile

__version__ = None
__minimum_client_version__ = None


def current_version():
    # If the version has already been read from the file, just return it.
    global __version__
    if __version__ is not None:
        return __version__

    # in the codes files.
    VFILE1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "VERSION"))
    # In the package files, after installing.
    VFILE2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ivert_data", "VERSION"))

    if os.path.exists(VFILE1):
        file_to_use = VFILE1
    elif os.path.exists(VFILE2):
        file_to_use = VFILE2
    else:
        raise FileNotFoundError("Could not find 'VERSION' file.")

    with open(file_to_use, 'r') as fobj:
        v = fobj.read().strip()
        return v


# Define the __version__ variable in this module.
__version__ = current_version()


def minimum_client_version():
    global __minimum_client_version__
    # If the version has already been read from the file, just return it.
    if __minimum_client_version__ is not None:
        return __minimum_client_version__

    # This is the minimum version of the client that the server will accept.
    # This is stored in ivert/VERSION_CLIENT_MIN
    MCVFILE1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "VERSION_CLIENT_MIN"))
    MCVFILE2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ivert_data", "VERSION_CLIENT_MIN"))
    if os.path.exists(MCVFILE1):
        file_to_use = MCVFILE1
    elif os.path.exists(MCVFILE2):
        file_to_use = MCVFILE2
    else:
        raise FileNotFoundError("Could not find 'VERSION_CLIENT_MIN' file.")

    with open(file_to_use, 'r') as f:
        # Strip off # comments and blank lines, then return the first line
        return [line.strip() for line in f.readlines() if line.strip()[0] != '#' and len(line.strip()) > 0][0]


__minimum_client_version__ = minimum_client_version()


def is_compatible(client_version: str) -> bool:
    """Returns True if the given client version is compatible with the server."""
    if not is_aws.is_aws():
        raise NotImplementedError("is_compatible is supported only on the AWS server. Use is_this_client_compatible instead.")
    return Version(client_version) >= Version(minimum_client_version())


def is_this_client_compatible():
    if is_aws.is_aws():
        raise NotImplementedError("is_this_client_compatible is supported only on the AWS client. Use is_compatible instead.")

    ivert_config = configfile.config()
    jobs_db_s3_key = ivert_config.s3_ivert_jobs_database_key

    # Initiate a boto3 session and client.
    client = boto3.Session(profile_name=ivert_config.aws_profile_ivert_export).client('s3')
    min_client_key = client.head_object(Bucket=ivert_config.s3_bucket_export, Key=jobs_db_s3_key)['Metadata'][ivert_config.s3_jobs_db_min_client_version_metadata_key]

    return Version(__version__) >= Version(min_client_key)


if __name__ == "__main__":
    print(__version__)
