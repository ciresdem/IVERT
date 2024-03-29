#!/usr/bin/env python3

import argparse
import boto3
import botocore.exceptions
import fnmatch
import glob
import os
import sys
import textwrap
import warnings

import utils.configfile

class S3_Manager:
    """Class for copying files into and out-of the IVERT AWS S3 buckets, as needed."""

    available_bucket_types = ("database", "input", "output")

    def __init__(self):
        self.config = utils.configfile.config()

        # Different buckets for each type.
        self.bucket_dict = {"database": self.config.s3_bucket_database,
                            "untrusted": self.config.s3_bucket_import_untrusted,
                            "trusted": self.config.s3_bucket_import_trusted,
                            "export": self.config.s3_bucket_export}

        # Different AWS profiles for each bucket. "None" indicates no profile is needed.
        # TODO: Replace with entries from the user profile created by ivert_new_user_setup.py
        self.bucket_profile_dict = {"database": None,
                                    "untrusted": self.config.aws_profile_ivert_ingest,
                                    "trusted": None,
                                    "export": self.config.aws_profile_ivert_export}

        self.session_dict = {"database": None,
                             "untrusted": None,
                             "trusted": None,
                             "export": None}

        # The s3 client. Client created on demand when needed by the :get_client() method.
        self.client_dict = {"database": None,
                            "untrusted": None,
                            "trusted": None,
                            "export": None}


    def get_resource_bucket(self, bucket_type="database"):
        """Return the open resource.BAD_FILE_TO_TEST_QUARANTINE.foobar

        If it doesn't exist yet, open one."""
        bucket_type = self.convert_btype(bucket_type)

        if self.session_dict[bucket_type] is None:
            self.session_dict[bucket_type] = boto3.Session(profile_name=self.bucket_profile_dict[bucket_type])

        return self.session_dict[bucket_type].resource("s3").Bucket(self.bucket_dict[bucket_type])

    def get_client(self, bucket_type="database"):
        """Return the open client.

        If it doesn't exist yet, open one."""
        bucket_type = self.convert_btype(bucket_type)

        if self.client_dict[bucket_type] is None:
            if self.session_dict[bucket_type] is None:
                self.session_dict[bucket_type] = boto3.Session(profile_name=self.bucket_profile_dict[bucket_type])
            self.client_dict[bucket_type] = self.session_dict[bucket_type].client("s3")

        return self.client_dict[bucket_type]


    def convert_btype(self, bucket_type):
        """Convert a user-entered bucket type into a valid value."""
        bucket_type = bucket_type.strip().lower()
        if bucket_type == 'd':
            bucket_type = 'database'
        elif bucket_type == 't':
            bucket_type = 'trusted'
        elif bucket_type == 'u':
            bucket_type = 'untrusted'
        elif bucket_type in ('e', 'x'):
            bucket_type = 'export'

        return bucket_type


    def get_bucketname(self, bucket_type="database"):
        bucket_type = self.convert_btype(bucket_type)

        if bucket_type.lower() not in self.bucket_dict.keys():
            # Try to see if it's a valid bucket name already, rather than a bucket_type.
            raise ValueError(f"Unknown bucket type '{bucket_type}'. Must be one of {list(self.bucket_dict.keys())}.")

        return self.bucket_dict[bucket_type]

    def verify_same_size(self, filename, s3_key, bucket_type="database"):
        """Return True if the local file is the exact same size in bytes as the S3 key."""
        head = self.exists(s3_key, bucket_type=bucket_type, return_head=True)
        if head is False:
            return False

        s3_size = int(head['ContentLength'])

        if os.path.exists(filename):
            local_size = os.stat(filename).st_size
        else:
            return False

        return s3_size == local_size

    def exists(self, s3_key, bucket_type="database", return_head=False):
        """Look in the appropriate bucket, and see if a file or directory exists there."""
        client = self.get_client(bucket_type=bucket_type)

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        if s3_key == "/":
            s3_key = ""

        try:
            head = client.head_object(Bucket=bucket_name, Key=s3_key)
            if return_head:
                return head
            else:
                return True

        except botocore.exceptions.ClientError as e:
            try:
                if self.is_existing_s3_directory(s3_key, bucket_type=bucket_type):
                    return True
            except botocore.exceptions.ClientError:
                pass

            if e.response["Error"]["Code"] == "404":
                return False
            else:
                warnings.warn(f"Warning: Unknown error fetching status of s3://{bucket_name}/{key}")
                return False

    def is_existing_s3_directory(self, s3_key, bucket_type="database"):
        "Return True if 'key' points to an existing directory (prefix) in the bucket. NOT a file. False otherwise."

        bucket_obj = self.get_resource_bucket(bucket_type=bucket_type)

        if bucket_obj and s3_key in (".", "", "/"):
            return True

        # Filter the list of objects by the prefix. If the prefix doesn't exist, this will be empty.
        objects = bucket_obj.objects.filter(Prefix=s3_key)

        # Loop through the objects returned.
        for i, obj in enumerate(objects):
            # If it's an exact match with the key, then it's a file (not a directory). Return False.
            if obj.key == s3_key:
                return False

            # Otherwise, the key should be the start of the object.
            assert obj.key.find(s3_key) == 0

            # If we match with an object and the character immediately after the prefix is a '/', then it's a directory.
            # If some other character is there, then we're not sure yet, move along to the next object.
            if s3_key[-1] == "/" or obj.key[len(s3_key)] == "/":
                return True

        return False

    def download(self, s3_key, filename, bucket_type="database", delete_original=False, fail_quietly=True):
        """Download a file from the S3 to the local file system."""
        client = self.get_client(bucket_type=bucket_type)

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        assert not self.contains_glob_flags(filename)

        if self.contains_glob_flags(s3_key):
            base_key = self.get_base_directory_before_glob(s3_key)
            all_s3_keys = self.listdir(base_key, bucket_type=bucket_type, recursive=True)
            matching_s3_keys = fnmatch.filter(all_s3_keys, s3_key)
            s3_keys_to_download = matching_s3_keys
        else:
            s3_keys_to_download = [s3_key]

        files_downloaded = []
        for s3k in s3_keys_to_download:
            # If the 'filename' given is a directory, use the same filename as the key, put the file in that directory.
            if os.path.isdir(filename):
                filename_to_download = os.path.join(filename, s3k.split("/")[-1])
            elif len(s3_keys_to_download) == 1:
                filename_to_download = filename
            else:
                raise ValueError(f"'{filename}' must be an existing directory if there are multiple keys to download.")

            client.download_file(bucket_name, s3k, filename_to_download)

            if not self.verify_same_size(filename_to_download, s3k, bucket_type=bucket_type):
                if fail_quietly:
                    continue
                else:
                    raise RuntimeError(f"Error: S3_Manager.download() failed to download file {s3k.split('/')[-1]} correctly.")

            files_downloaded.append(filename_to_download)

            if delete_original:
                client.delete_object(Bucket=bucket_name, Key=s3k)

        return files_downloaded

    def upload(self,
               filename,
               s3_key,
               bucket_type="database",
               delete_original=False,
               fail_quietly=True,
               include_md5=False):
        """Upload a file from the local file system to the S3."""
        client = self.get_client(bucket_type=bucket_type)

        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        assert not self.contains_glob_flags(s3_key)

        if self.contains_glob_flags(filename):
            matching_filenames = glob.glob(filename)
        else:
            matching_filenames = [filename]

        if len(matching_filenames) == 0:
            return []

        files_uploaded = []
        for fname in matching_filenames:
            # If the key is pointing to an S3 directory (prefix), then use the same filename as filename, and put it in that
            # directory.
            if self.is_existing_s3_directory(s3_key, bucket_type=bucket_type) or s3_key[-1] == "/":
                s3_key = (s3_key + "/" + os.path.basename(fname)).replace("//", "/")

            client.upload_file(fname, bucket_name, s3_key)

            if not self.verify_same_size(fname, s3_key, bucket_type=bucket_type):
                if fail_quietly:
                    return False
                else:
                    raise RuntimeError("Error: S3_Manager.upload() failed to upload file correctly.")

            files_uploaded.append(s3_key)

            if delete_original:
                os.remove(fname)

        return files_uploaded

    def listdir(self, s3_key, bucket_type="database", recursive=False):
        """List all the files within a given directory.

        Returns the full key path, since that's how S3's operate. But this make it a bit different than os.listdir().
        """
        # Directories should not start with '/'
        if len(s3_key) > 0 and s3_key[0] == "/":
            s3_key = s3_key[1:]

        # If we're using glob flags, return all the matching keys
        if self.contains_glob_flags(s3_key):
            s3_base_key = self.get_base_directory_before_glob(s3_key)
            s3_matches_all = self.listdir(s3_base_key, bucket_type=bucket_type, recursive=True)
            s3_matches = fnmatch.filter(s3_matches_all, s3_key)
            return s3_matches

        # If no glob flags, make sure it's actually a directory we're looking at, not just a random matching substring.
        if not self.is_existing_s3_directory(s3_key, bucket_type=bucket_type):
            raise FileNotFoundError(f"'{s3_key}' is not a directory in bucket '{self.get_bucketname(bucket_type=bucket_type)}'.")

        # Make sure the directory ends with a '/'.
        if not s3_key.endswith("/"):
            s3_key = s3_key + "/"

        bucket_obj = self.get_resource_bucket(bucket_type=bucket_type)
        # If we're recursing, just return everything that is in that Prefix.
        if recursive:
            files = bucket_obj.objects.filter(Prefix=s3_key).all()
            return [obj.key for obj in files]
        else:
            # Get the full string that occurs after the directory listed, for each subset.
            result = self.get_client(bucket_type=bucket_type).list_objects(Bucket=self.get_bucketname(bucket_type=bucket_type),
                                                                           Prefix=s3_key,
                                                                           Delimiter="/")

            if "CommonPrefixes" in result.keys():
                subdirs = [subdir["Prefix"] for subdir in result["CommonPrefixes"]]
            else:
                subdirs = []

            if "Contents" in result.keys():
                files = [f["Key"] for f in result["Contents"]]
            else:
                files = []

            return subdirs + files

    def delete(self, s3_key, bucket_type="database"):
        """Delete a key (file) from the S3."""
        client = self.get_client(bucket_type=bucket_type)
        bucket_name = self.get_bucketname(bucket_type=bucket_type)

        if self.contains_glob_flags(s3_key):
            matching_keys = self.listdir(s3_key, bucket_type=bucket_type, recursive=True)
            for k in matching_keys:
                client.delete_object(Bucket=bucket_name, Key=k)
        else:
            client.delete_object(Bucket=bucket_name, Key=s3_key)

        return

    @staticmethod
    def contains_glob_flags(s3_key):
        """Return True if a string contains any glob-style wildcard flags."""
        return ("*" in s3_key) or ("?" in s3_key) or ("[" in s3_key)

    @staticmethod
    def get_base_directory_before_glob(s3_key):
        """Return the base directory before any glob-style wildcard flags in an S3 key."""
        dirs = s3_key.split("/")
        basedirs = []
        for dname in dirs:
            if "*" in dname or "?" in dname or "[" in dname:
                break
            basedirs.append(dname)

        return "/".join(basedirs)


def define_and_parse_args():
    # TODO: Replace this with an argument parser that uses subparsers for various commands.
    parser = argparse.ArgumentParser(description="Quick python utility for interacting with IVERT's S3 buckets.")
    parser.add_argument("command", nargs="+", help=f"The command to run. Options are 'ls', 'rm', 'cp', or 'mv'."
                        " Run each command without arguments to see usage messages for it.")
    parser.add_argument("--bucket", "-b", default="database", help=
                        "The shorthand for which ivert S3 bucket we're pulling from, or the explicit name of a bucket."
                        " Shorthand options are 'database' or 'd' (s3 bucket of the IVERT database & other core data),"
                        " 'trusted' or 't' (the S3 bucket for input files that just passed secure ingest),"
                        " 'untrusted' or 'u' (s3 bucket for pushing files into IVERT),"
                        " and 'export' or 'e' or 'x' (where IVERT puts output files to disseminate). These are abstractions."
                        " The actual S3 bucket names for each category are defined in ivert_config.ini."
                        " Default: 'database'")
    parser.add_argument("--recursive", "-r", default=False, action="store_true", help=
                        "For the 'ls' command, list all the files recursively, including in all sub-directories.")

    return parser.parse_args()


if __name__ == "__main__":
    args = define_and_parse_args()

    # This optional parameter appears in most
    bucketopt_message = \
    "\n  --bucket BUCKET, -b BUCKET\n" + \
    textwrap.fill("Tag for the IVERT bucket type being used. Options are database', 'untrusted',"
                  " 'trusted', and 'export'. The actual names of the buckets are pulled from"
                  " ivert_config.ini. Default is 'database', so commands will (by default) be referenced to the"
                  " s3 bucket where the IVERT database resides.",
                  width=os.get_terminal_size().columns,
                  initial_indent=' ' * 20,
                  subsequent_indent=' ' * 20)

    command = args.command
    if command[0] == "ls":
        if len(command) > 2:
            raise ValueError("'ls' should have exactly 0 or 1 positional argument after it.")
        # The "s3:" is not mandatory. Strip it off if it exists.

        if len(command) == 1:
            print("usage: python {0} ls s3_prefix [--recursive] [--bucket BUCKET]".format(os.path.basename(__file__)) +
                  "\n\nList all the files in that prefix directory." +
                  "\n\npositional arguments:" +
                  textwrap.fill("\n  s3_prefix       The directory (called a 'prefix' in s3) in which to list all files"
                                " present. Prints the full keyname (with prefix directories). Use an empty prefix"
                                " ('s3:', '.', or '/') to list files in the root directory of the bucket.",
                                width=os.get_terminal_size().columns,
                                subsequent_indent=' ' * 20) +
                  "\n\noptions:" +
                  textwrap.fill("  --recursive, -r Recursively list all files in that directory, including within"
                                "sub-folders.",
                                width=os.get_terminal_size().columns,
                                subsequent_indent=' ' * 20) +
                  bucketopt_message
                  )
            sys.exit(0)

        # if len(command) == 1:
        #     command = command + [""]

        key = command[1].lstrip("s3:").lstrip("S3:").strip()
        if key == "/" or key == ".":
            key = ""

        s3m = S3_Manager()

        results = s3m.listdir(key, bucket_type=args.bucket, recursive=args.recursive)
        for line in results:
            print(line)

    elif command[0] == "rm":
        if len(command) == 1:
            print("usage: python {0} rm s3_key [--bucket BUCKET]".format(os.path.basename(__file__)) +
                  "\n\nRemove a file from the s3." +
                  "\n\npositional arguments:" +
                  "\n  s3_key          The file to remove." +
                  "\n\noptions:" +
                  bucketopt_message
                  )
            sys.exit(0)

        s3m = S3_Manager()

        for key in command[1:]:
            # The "s3:" is not mandatory. Strip it off if it exists.
            key = key.lstrip("s3:").lstrip("S3:")
            s3m.delete(key, bucket_type=args.bucket)

    elif command[0] in ("cp", "mv"):
        if len(command) == 1:
            move_or_copy = "copy" if (command[0] == "cp") else "move"
            print("usage 1: python {0} {1} s3:s3_key file_or_directory [--bucket BUCKET]".format(os.path.basename(__file__), command[0]) +
                  f"\n             {move_or_copy.capitalize()} a file from the s3 into the local file system." +
                  "\n\nusage 2: python {0} {1} file s3:s3_key_or_prefix [--bucket BUCKET]".format(os.path.basename(__file__), command[0]) +
                  f"\n             {move_or_copy.capitalize()} a local file into the s3." +
                  "\n\nOne of the positional arguments must start with 's3:' to identify which argument corresponds to the s3 bucket."
                  "The other is assumed to be a local file."
                  "\n\npositional arguments (only two are used in any command, one s3: argument and another local):" +
                  "\n  s3:s3_key            A file in the s3 bucket." +
                  "\n  s3:s3_key_or_prefix  A file or directory (prefix) in the s3 bucket. Use an empty prefix ('s3:')"
                  f"\n                       to {move_or_copy} files into the root directory of the bucket." +
                  "\n  file                 A file on the local file system." +
                  "\n  file_or_directory    A file or directory on the local file system."
                  "\n\noptions:" +
                  bucketopt_message
                  )
            sys.exit(0)

        s3m = S3_Manager()

        if len(command) < 3:
            raise ValueError(f"'{command[0]}' should be followed by exactly 2 files, one of them preceded by 's3:'")

        c1 = command[1]
        c2 = command[-1]

        local_files = [fn for fn in command[1:] if fn.lower().find("s3:") == -1]
        s3_files = [fn for fn in command[1:] if fn.lower().find("s3:") == 0]

        move_or_copy = "copy" if (command[0] == "cp") else "move"

        # In the case of local wildcards, the shell will expand the list of files before sending it to python.
        # This is only valid if the local files came first (with wildcards) followed by the s3 file. Handle this case.

        if len(s3_files) != 1:
            raise ValueError(f"'{command[0]}' should be followed by 2 files, exactly one of them preceded by 's3:'")
        if len(local_files) > 1 and s3_files[0] == c1:
            raise ValueError(f"Cannot {move_or_copy} to more than 1 local file or directory.")

        s3_file = s3_files[0]

        # Determine if we're uploading or downloading. Which one came first?
        if s3_file == c1:
            downloading = True
        else:
            downloading = False

        # Trim off the "s3:" in front of the s3 file.
        s3_file = s3_file.lstrip("s3:").lstrip("S3:").strip()
        if (s3_file == "") and downloading:
            raise ValueError(f"Cannot {move_or_copy} the base directory (s3:) of the s3 bucket. Try using a wildcard (*).")

        for local_file in local_files:
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
