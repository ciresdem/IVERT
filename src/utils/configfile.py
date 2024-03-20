# -*- coding: utf-8 -*-
import ast
import configparser
# If I import this script from a script in the parent directory, the "import is_aws" breaks. Instead, use "import utils/is_aws"
try:
    import is_aws
except ModuleNotFoundError:
    import utils.is_aws as is_aws
import os
import re
import sys

class config:
    """A subclass implementation of configparser.ConfigParser(), expect that config attributes are referenced as object
    attributes rather than in a dictionary.

    So if the .ini file contains the attribute:
         varname = 0
    it is referenced by:
         >> c = configfile.config()
         >> c.varname
         0

    When initialized, it will check whether it is running in an AWS (Amazon Web Services) cloud environment
    and if so, use the [AWS] section of the configfile.

    All paths are considered relative to the location of the configfile. Absolute paths will be left unchanged.
    All other paths that contain a file-delimeter character ("/" on linux, "\" on Windows) will be joined with the
    path of the configfile and converted to an absolute path.
    The EXCEPTION ot the above rule is if the variable name begins with "s3_", in which case it is assumed to be
    an AWS S3 bucket prefix and will not be converted to an absolute path on the local machine.

    The two sections in the .ini configfile should be [DEFAULT] and [AWS].
    No other sections are read by this object, for now.
    """

    def __init__(self,
                 configfile=os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                         "..", "..", "config", "ivert_config.ini"))):

        self._configfile = os.path.abspath(configfile)
        # print(self._configfile)
        self._config = configparser.ConfigParser()
        self.is_aws = is_aws.is_aws()

        self._config.read(configfile)

        # Turn the values of the config file into attributes.
        # This does not handle sections separately. Change this functionality
        # if I need to use different sections separately.
        self._parse_config_into_attrs()

    def _abspath(self, path, only_if_actual_path_doesnt_exist=False):
        """In this project, absolute paths are relative to the location of the
        configfile. In this case. join them with the path to the config file and
        return an absolute path rather than a relative path."""
        # If we've specified to do this only if the path doesn't exist in its current location,
        # and the path does exist in its current location (either the filename, or the parent directory),
        # then just return the path as-is
        if only_if_actual_path_doesnt_exist and (os.path.exists(path) or os.path.exists(os.path.split(path)[0])):
            return path

        return os.path.abspath(os.path.join(os.path.dirname(self._configfile), path))

    def _parse_config_into_attrs(self):
        """Read all the config lines, put into object attributes. If we're running in an AWS instance, also read the
        [AWS] section.
        """
        # First input the default values from the config file.
        for k, v in self._config["DEFAULT"].items():
            self._read_option(k, v)

        # Then, if we're running in an AWS environment, read all the values from the [AWS] section.
        if self.is_aws:
            section = self._config["AWS"]
            for k, v in section.items():
                self._read_option(k, v)

    def _read_option(self, key, value):
        """Read an individual option.

        Will sipmly use the "eval" python command to parse it,
        and then attempt to read as a boolean if that fails. It helps to keep the
        .ini file a python-readable format, and allows base python objects to be in there.
        """
        try:
            # Using ast.literal_eval() rather than eval(), because literal_eval only allows the creation of generic
            # python objects but doesn't allow the calling of functions or commands that could pose security risks.
            # It will natively evaluate things like lists, dictionaries, or other generic python data types.
            setattr(self, key, ast.literal_eval(value))
            return
        except (NameError, ValueError, SyntaxError):
            pass

        # In some boolean cases, you can put other things besides "True/False", such as "yes/no"
        # Use configparser's intelligence to try to interpret it as a boolean.
        try:
            setattr(self, key, self._config.getboolean("DEFAULT", key))
            return
        except (NameError, ValueError, SyntaxError):
            pass

        # Check to see if this is potentially a path. Interpret it as such if it is a string and contains path
        # characters ('\' in Windows or '/' in Linux).
        # If this is the case, return the absolute path of that file/directory *relative* to the current directory the
        # config.ini file is contained.
        try:
            if key[:3].lower() == "s3_":
                # This is an S3 key-path. Do not convert it to an absolute path.
                pass

            elif sys.platform in ('linux', 'cygwin', 'darwin') and value.find("/") > -1:
                # If it's an absolute path or it already exists where it is, just use it as-is
                if value.strip().find("/") == 0:
                    setattr(self, key, os.path.abspath(value))
                # If it references the home directory, expand that on the local machine.
                elif value.find("~") > -1:
                    setattr(self, key, os.path.abspath(os.path.expanduser(value)))
                # If it's a relative path, make it relative to the _configfile's local directory.
                else:
                    setattr(self, key, self._abspath(os.path.join(os.path.dirname(self._configfile), value)))
                return

            elif sys.platform in ('win32', 'win64') and value.find("\\") > -1:
                # If it's an absolute path or it already exists where it is, just use it as-is
                # For a base path, look for the "C:\" drive-name pattern at the start (upper- or lower-case).
                if re.search(r'\A[A-Za-z]\:\\', value.strip()) is not None:
                    setattr(self, key, os.path.abspath(value))
                # If it references the home directory, expand that on the local machine.
                elif value.find("~") > -1:
                    setattr(self, key, os.path.abspath(os.path.expanduser(value)))
                # If it's a relative path, make it relative to the _configfile directory.
                else:
                    setattr(self, key, self._abspath(os.path.join(os.path.dirname(self._configfile), value)))
                return
        except ValueError:
            pass

        # Otherwise, it's probably just a string value, set it as-is.
        setattr(self, key, value)
        return

    def _fill_bucket_names(self):
        """Fills in the bucket name entries in the config file.

        We do not store any of these bucket names in the IVERT public Github, so they are filled in at runtime.

        Specifically, we're looking for:
            - [S3_BUCKET_DATABASE]
            - [S3_BUCKET_UNTRUSTED]
            - [S3_BUCKET_TRUSTED]
            - [S3_BUCKET_EXPORT]

        If we're client-side, we can fill in at most two of these: [S3_BUCKET_UNTRUSTED] and [S3_BUCKET_EXPORT], which
        are set during IVERT setup and stored in the user config file.

        If we're server-side, we need to fill in [S3_BUCKET_DATABASE], [S3_BUCKET_TRUSTED], and [S3_BUCKET_EXPORT].
        These can be found in the ivert_setup/setup/paths.sh file from the ivert_setup repository."""

        # TODO: Finish this.

    def _add_user_variables_to_config(self):
        """Add the names of the S3 buckets to the configfile.config object.

        On a client instance, ivert setup needs to be run to flesh out the user configfile, before this will work."""
        # Make sure all these are defined in here. They may be assigned to None but they should exist. This is
        # a sanity check in case we changed the bucket variables names in the configfile.
        assert hasattr(self, "s3_bucket_ipmort_untrusted")
        assert hasattr(self, "s3_bucket_import_trusted")
        assert hasattr(self, "s3_bucket_export")
        assert hasattr(self, "s3_bucket_database")
        assert hasattr(self, "user_email")
        assert hasattr(self, "username")
        assert hasattr(self, "aws_profile_ivert_ingest")
        assert hasattr(self, "aws_profile_ivert_export")

        # If we're on the client side (not in an AWS instance), get this from the user configfile.
        #    In this case, only the s3_bucket_import_untrusted and s3_bucket_export are needed.
        # TODO: Fetch the bucket names from the user configfile.
        # TODO: Also get the user email, username, and aws_profiles from the user configfile.
        if self.is_aws:
            pass

        # If we're on the server side (in the AWS), get these from the "ivert_setup" repository under /setup/paths.sh.
        #    In this case, only the s3_bucket_import_trusted, s3_bucket_database, and s3_bucket_export are needed.
        # TODO: Fetch the bucket names from the ivert_setup/setup/paths.sh repository file.
        else:
            pass

        return
