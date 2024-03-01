import argparse
import boto3
import botocore.exceptions
import warnings

import utils.configfile

class S3_Manager:
    """Class for copying files into and out-of the IVERT AWS S3 buckets, as needed."""

    def __init__(self):
        self.config = utils.configfile.config()
        assert self.config._is_aws
        # Different buckets for each type.
        self.bucket_dict = {"database": self.config.s3_name_database,
                            "inputs": self.config.s3_name_inputs,
                            "outputs": self.config.s3_name_outputs}
        self.client = None


    def get_client(self):
        "Return the open client. If it doesn't exist yet, open one."
        if self.client is not None:
            return self.client
        self.client = boto3.client("s3")
        return self.client

    def get_bucketname(self, bucket_type="database"):
        if bucket_type.lower() not in self.bucket_dict.keys():
            raise ValueError(f"Unknown bucket type '{bucket_type}'. Must be one of {list(self.bucket_dict.keys())}.")

        return self.bucket_dict[bucket_type]

    def exists(self, key, bucket_type="database"):
        """Look in the appropriate bucket, and see if a file exists there."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        try:
            head = client.head_object(Bucket=bucket_name, Key=key)
            print(head)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                warnings.warn(f"Warning: Unknown error fetching status of s3://{bucket_name}/{key}")
                return False

    def download(self, key, filename, bucket_type="database", delete_original=False):
        """Download a file from the S3 to the local file system."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        response = client.download_file(bucket_name, key, filename)

        if delete_original:
            client.delete_object(Bucket=bucket_name, Key=key)

        return response

    def upload(self, filename, key, bucket_type="database", delete_original=False):
        """Upload a file from the local file system to the S3."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        response = client.upload_file(filename, bucket_name, key)

        if delete_original:
            os.remove(filename)

        return response

    def listdir(self, key, bucket_type="database"):
        """List all the files within a given directory.

        NOTE: This lists all objects recursively, even in sub-directories, so it doesn't behave exactly like os.listdir.
        """
        # client = self.get_client()
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        resource = boto3.resource("s3")
        bucket = resource.Bucket(bucket_name)
        files = bucket.objects.filter(Prefix=key).all()
        return [obj.key for obj in files]


def define_and_parse_args()

if __name__ == "__main__":
    s3 = S3_Manager()