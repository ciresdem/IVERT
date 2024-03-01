import argparse
import boto3
import botocore.exceptions
import os
import warnings

import utils.configfile

class S3_Manager:
    """Class for copying files into and out-of the IVERT AWS S3 buckets, as needed."""

    available_bucket_types = ("database", "input", "output")

    def __init__(self):
        self.config = utils.configfile.config()
        assert self.config._is_aws
        # Different buckets for each type.
        self.bucket_dict = {"database": self.config.s3_name_database,
                            "input": self.config.s3_name_inputs,
                            "output": self.config.s3_name_outputs}
        self.client = None


    def get_client(self):
        "Return the open client. If it doesn't exist yet, open one."
        if self.client is not None:
            return self.client
        self.client = boto3.client("s3")
        return self.client

    def get_bucketname(self, bucket_type="database"):
        bucket_type = bucket_type.strip().lower()

        if bucket_type.lower() not in self.bucket_dict.keys():
            # Try to see if it's a valid bucket name already, rather than a bucket_type.
            raise ValueError(f"Unknown bucket type '{bucket_type}'. Must be one of {list(self.bucket_dict.keys())}.")

        return self.bucket_dict[bucket_type]

    def verify_same_length(self, filename, key, bucket_type="database"):
        """Return True if the local file is the exact same length as the S3 key."""
        head = self.exists(key, bucket_type=bucket_type)
        if head is False:
            return False

        s3_size = int(head['content-length'])

        if os.path.exists(filename):
            local_size = os.stat(filename).st_size
        else:
            return False

        return s3_size == local_size

    def exists(self, key, bucket_type="database", return_head=False):
        """Look in the appropriate bucket, and see if a file or directory exists there."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        try:
            head = client.head_object(Bucket=bucket_name, Key=key)
            if return_head:
                return head
            else:
                return True

        except botocore.exceptions.ClientError as e:
            try:
                if self.is_existing_s3_directory(key, bucket_type=bucket_type):
                    return True
            except botocore.exceptions.ClientError:
                pass

            if e.response["Error"]["Code"] == "404":
                return False
            else:
                warnings.warn(f"Warning: Unknown error fetching status of s3://{bucket_name}/{key}")
                return False

    def is_existing_s3_directory(self, key, bucket_type="database"):
        "Return True if 'key' points to an existing directory (prefix) in the bucket. NOT a file. False otherwise."
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        resource = boto3.resource('s3')
        bucket = resource.Bucket(bucket_name)

        objects = bucket.objects.filter(Prefix=key)

        i = -1
        # Try to loop through the first object. If the prefix doesn't exist this will be empty.
        for i, obj in enumerate(objects):
            # This "break on the first instance" thing seems stupid, but I don't yet know how to just get the first
            # object of the filter() method back, besides iterating over it.
            if i == 0:
                break

        # If we didn't enter the loop (i.e. there were no matching objects) then return False.
        if i == -1:
            return False
        # If it's an exact match with the key, then it's a file (not a directory). Return False.
        if obj.key == key:
            return False

        # Otherwise, the key should be the start of the first object there.
        assert obj.key.find(key) == 0

        # To make sure we don't just match any substring of the key, make sure that either the key ends in "/" or
        # the character right after it in the matching prefix is "/".
        if (key[-1] == "/") or (obj.key[len(key)] == "/"):
            return True


    def download(self, key, filename, bucket_type="database", delete_original=False, fail_quietly=True):
        """Download a file from the S3 to the local file system."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        # If the 'filename' given is a directory, use the same filename as the key, put the file in that directory.
        if os.path.isdir(filename):
            filename = os.path.join(filename, key.split("/")[-1])

        response = client.download_file(bucket_name, key, filename)

        if not self.verify_same_length(filename, key, bucket_type=bucket_type):
            if fail_quietly:
                return False
            else:
                raise RuntimeError("Error: S3_Manager.download() failed to download file correctly.")

        if delete_original:
            client.delete_object(Bucket=bucket_name, Key=key)

        return response

    def upload(self, filename, key, bucket_type="database", delete_original=False, fail_quietly=True):
        """Upload a file from the local file system to the S3."""
        client = self.get_client()

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        # If the key is pointing to an S3 directory (prefix), then use the same filename as filename, and put it in that
        # directory.
        if self.is_existing_s3_directory(key, bucket_type=bucket_type):
            key = key + "/" + os.path.basename(filename)

        response = client.upload_file(filename, bucket_name, key)

        if not self.verify_same_length(filename, key, bucket_type=bucket_type):
            if fail_quietly:
                return False
            else:
                raise RuntimeError("Error: S3_Manager.upload() failed to upload file correctly.")

        if delete_original:
            os.remove(filename)

        return response

    def listdir(self, key, bucket_type="database", recursive=False):
        """List all the files within a given directory.

        NOTE: This lists all objects recursively, even in sub-directories, so it doesn't behave exactly like os.listdir.
        """
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        resource = boto3.resource("s3")
        bucket = resource.Bucket(bucket_name)
        files = bucket.objects.filter(Prefix=key).all()
        return [obj.key for obj in files]

    def delete(self, key, bucket_type="database"):
        """Delete a key (file) from the S3."""
        client = self.get_client()
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        return client.delete_object(Bucket=bucket_name, Key=key)


def define_and_parse_args():
    parser = argparse.ArgumentParser(description="Quick python utility for interacting with IVERT's S3 buckets.")
    parser.add_argument("command", help="""The command to run. Options are:
       ls [prefix] -- List all the files in that prefix directory. Use --recursive (-r) to recursively get all the files.
       rm [key] :                   Remove a file from the S3.
       cp [key] [filename_or_dir] : Copy a file from the S3 to a local file.
       cp [filename] [key] :        Copy a local file into the S3. If the key is a
                                    prefix (directory), copy it into that directory/prefix.
       mv [key] [filename_or_dir] : Move a file from the S3 to a local file. Delete the
                                    original in the S3.
       mv [filename] [key] :        Move a local file into the S3. Delete the original.
                                    If key is a prefix (directory), copy it into that prefix.
    """)
    parser.add_argument("--bucket", "-b", default="database", help=
                        "The shorthand for which ivert S3 bucket we're pulling from, or the explicit name of a bucket."
                        "Shorthand options are 'database' "
                        "(location of the IVERT database & other core data), 'input' (the S3 bucket for inpt files that"
                        "just passed secure ingest), 'output' (where IVERT puts output files to disseminate). These are "
                        "abstractions. The actual S3 bucket names for each category are defined in ivert_config.ini.")
    parser.add_argument("--recursive", "-r", default=False, action="store_true", help=
                        "For the 'ls' command, list all the files recursively, including all sub-directories.")


if __name__ == "__main__":
    s3 = S3_Manager()