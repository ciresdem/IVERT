import argparse
import boto3
import botocore.exceptions
import os
import sys
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
        if key == "":
            return True

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
        else:
            return False

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

        Returns the full key path, since that's how S3's operate. But this make it a bit different than os.listdir().

        NOTE: This lists all objects recursively, even in sub-directories, so it doesn't behave exactly like os.listdir.
        """
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        # Directories must end in '/' for an S3 bucket.
        if len(key) > 0 and key[-1] != "/":
            key = key + "/"

        # First make sure it's actually a directory we're looking at, not just a random matching substring.
        if not self.is_existing_s3_directory(key, bucket_type=bucket_type):
            raise FileNotFoundError(f"'{key}' is not a directory in bucket '{bucket_name}'")

        resource = boto3.resource("s3")
        bucket = resource.Bucket(bucket_name)
        # If we're recursing, just return everything.
        if recursive:
            files = bucket.objects.filter(Prefix=key).all()
            return [obj.key for obj in files]
        else:
            # Get the full string that occurs after the directory listed, for each subset.
            result = self.get_client().list_objects(Bucket=bucket_name, Prefix=key, Delimiter="/")
            subdirs = [subdir["Prefix"] for subdir in result["CommonPrefixes"]]
            files = [f["Key"] for f in result["Contents"]]
            return subdirs + files

    def delete(self, key, bucket_type="database"):
        """Delete a key (file) from the S3."""
        client = self.get_client()
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        return client.delete_object(Bucket=bucket_name, Key=key)


def define_and_parse_args():
    parser = argparse.ArgumentParser(description="Quick python utility for interacting with IVERT's S3 buckets.")
    parser.add_argument("command", nargs="+", help=f"The command to run. Options are 'ls', 'rm', 'cp', or 'mv'.  "
                        "'ls' and 'rm' are followed by 1 argument: a prefix-directory (for 'ls') or a full file key (for 'rm').  "
                        "'rm' and 'cp' are followed by 2 arguments, one of which must be prefixed by 's3:' to indicate "
                        "which identifier is associated with the S3 Bucket. The name of the S3 bucket does NOT need to be "
                        "included. Use the '--bucket | -b' argument to specify a specific IVERT bucket.")
    # Type 'python {os.path.basename(__file__)} [command] -h' for help on a particular command.""")
    parser.add_argument("--bucket", "-b", default="database", help=
                        "The shorthand for which ivert S3 bucket we're pulling from, or the explicit name of a bucket."
                        "Shorthand options are 'database' "
                        "(location of the IVERT database & other core data), 'input' (the S3 bucket for inpt files that"
                        "just passed secure ingest), 'output' (where IVERT puts output files to disseminate). These are "
                        "abstractions. The actual S3 bucket names for each category are defined in ivert_config.ini."
                        " Default: 'database'")
    parser.add_argument("--recursive", "-r", default=False, action="store_true", help=
                        "For the 'ls' command, list all the files recursively, including in all sub-directories.")

    return parser.parse_args()


if __name__ == "__main__":
    args = define_and_parse_args()

    s3m = S3_Manager()

    command = args.command
    if command[0] == "ls":
        if len(command) > 2:
            raise ValueError("'ls' should have exactly 0 or 1 positional argument after it.")
        # The "s3:" is not mandatory. Strip it off if it exists.

        # if command[1] in ("-h", "--help"):
        #     print("python s3.py ls S3_DIRNAME [--recursive | -r] [--bucket | -b BUCKET]"
        #           "\n            List all the files in that prefix directory."
        #           "\n            Add --recursive or -r to recursively list all files, including files in sub-directories.")
        #     sys.exit(0)

        if len(command) == 1:
            command = command + [""]

        key = command[1].lstrip("s3:").lstrip("S3:")
        results = s3m.listdir(key, bucket_type=args.bucket, recursive=args.recursive)
        for line in results:
            print(line)

    elif command[0] == "rm":
        if len(command) == 1:
            raise ValueError("'rm' should be followed by at least one file key.")

        # if command[1] in ("-h", "--help"):
        #     print("python s3.py rm S3_FILENAME  [--bucket | -b BUCKET]"
        #           "\n            Remove a file from the S3.")
        #     sys.exit(0)

        for key in command[1:]:
            # The "s3:" is not mandatory. Strip it off if it exists.
            key = key.lstrip("s3:").lstrip("S3:")
            s3m.delete(key, bucket_type=args.bucket)

    elif command[0] in ("cp", "mv"):
        # The only difference between "cp" and "mv" is whether we delete the original file after copying over.
        # handle them both here.
        # if len(command) > 1 and command[1] in ("-h", "--help"):
        #     print(f"python s3.py {command[0]} s3:S3_FILENAME LOCAL_FILE_OR_DIR [--bucket | -b BUCKET]"
        #           "\n   or"
        #           f"python s3.py {command[0]} LOCAL_FILE s3:S3_FILENAME_OR_DIR [--bucket | -b BUCKET]" +
        #           "\n            {0}".format("Copy" if command[0] == "cp" else "Move") +
        #           " a file to/from the S3."
        #           "\n            The S3 location must be preceded by an 's3:' prefix."
        #           "\n            Only one entry should contain this 's3:' prefix.")

        if len(command) != 3:
            raise ValueError(f"'{command[0]}' should be followed by exactly 2 files, one of them preceded by 's3:'")
        c1 = command[1]
        c2 = command[2]

        local_file = [fn for fn in [c1, c2] if fn.lower().find("s3:") == -1]
        s3_file = [fn for fn in [c1, c2] if fn.lower().find("s3:") == 0]

        if len(s3_file) != 1 or len(local_file) != 1:
            raise ValueError(f"'{command[0]}' should be followed by exactly 2 files, one of them preceded by 's3:'")

        # Determine if we're uploading or downloading. Which one came first?
        downloading = True
        if local_file[0] == command[1]:
            downloading = False

        local_file = local_file[0]
        # Trim off the "s3:" in front of the s3 file.
        s3_file = s3_file[0][3:]

        if downloading:
            s3m.download(s3_file,
                         local_file,
                         bucket_type=args.bucket,
                         delete_original=(command[0] == "mv"),
                         fail_quietly=False)
        else:
            s3m.upload(local_file,
                       s3_file,
                       bucket_type=args.bucket,
                       delete_original=(command[0] == "mv"),
                       fail_quietly=False)

    else:
        raise ValueError(f"Unhandled command '{command[0]}'")
