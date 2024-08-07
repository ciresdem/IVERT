import boto3
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


def fetch_min_client_from_server(ivert_config=None):
    if ivert_config is None:
        ivert_config = configfile.config()

    jobs_db_s3_key = str(ivert_config.s3_ivert_jobs_database_key)

    # Initiate a boto3 session and client.
    client = boto3.Session(profile_name=str(ivert_config.aws_profile_ivert_export)).client('s3')
    return client.head_object(Bucket=str(ivert_config.s3_bucket_export), Key=jobs_db_s3_key)['Metadata'][ivert_config.s3_jobs_db_min_client_version_metadata_key]


def is_this_client_compatible():
    if is_aws.is_aws():
        raise NotImplementedError("is_this_client_compatible is supported only on the AWS client. Use is_compatible instead.")

    ivert_config = configfile.config()
    min_client_key = fetch_min_client_from_server(ivert_config=ivert_config)

    return Version(ivert_config.ivert_version) >= Version(min_client_key)
