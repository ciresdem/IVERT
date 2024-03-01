import boto3
import botocore
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

    def exists(self, key, s3_bucket_category='database', bucket_type="database"):
        """Look in the appropriate bucket, and see if a file exists there."""
        client = self.get_client()

        if bucket_type.lower() not in self.bucket_dict.keys():
            raise ValueError(f"Unknown bucket type '{bucket_type}'. Must be one of {list(self.bucket_dict.keys())}.")

        bucket_name = self.bucket_dict[bucket_type]

        try:
            client.head_object(Bucket=bucket_name, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise RuntimeWarning(f"Error fetching status of s3://{bucket_name}/{key}")
                return False

        return False


if __name__ == "__main__":
    s3 = S3_Manager()