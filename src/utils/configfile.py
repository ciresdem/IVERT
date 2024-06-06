# -*- coding: utf-8 -*-
import ast
import configparser
import os
import re
import sys

if vars(sys.modules[__name__])['__package__'] == 'ivert_utils':
    # When this is built a setup.py package, it names the modules 'ivert' and 'ivert_utils'. This reflects that.
    import ivert_utils.is_aws as is_aws
    import ivert_utils.version as version
else:
    # If running as a script, import this way.
    try:
        import is_aws
        import version
    except ModuleNotFoundError:
        import utils.is_aws as is_aws
        import utils.version as version

ivert_default_configfile = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                        "..", "..", "config", "ivert_config.ini"))
# When we build the ivert package, this is the location of the ivert_data directory. Look for it there.
if not os.path.exists(ivert_default_configfile):
    ivert_default_configfile = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                            "..", "..", "..", "..", "ivert_data", "config", "ivert_config.ini"))

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
                 configfile=ivert_default_configfile):
        """Initializes a new instance of the config class."""

        self._configfile = os.path.abspath(os.path.realpath(configfile))
        self._config = configparser.ConfigParser()
        self.is_aws = is_aws.is_aws()

        if not os.path.exists(configfile):
            raise FileNotFoundError(f"Configfile {configfile} not found.")

        self._config.read(configfile)

        # Turn the values of the config file into attributes.
        # This does not handle sections separately. Change this functionality
        # if I need to use different sections separately.
        self._parse_config_into_attrs()

        # If we're importing the primary IVERT config file, add the user variables and S3 creds to the config as well.
        if os.path.basename(self._configfile) == os.path.basename(ivert_default_configfile):
            self._add_user_variables_and_s3_creds_to_config_obj()

        if hasattr(self, "ivert_version") and self.ivert_version is None:
            self.ivert_version = version.__version__


    def _abspath(self, path, only_if_actual_path_doesnt_exist=False):
        """Retreive the absolute path of a file path contained in the configfile.

        In this project, absolute paths are relative to the location of the
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

        # Then, if we're running in an AWS environment, read all the values from the [AWS] section (if it exists).
        if self.is_aws and ("AWS" in self._config):
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

    def _fill_bucket_names_from_ivert_setup(self, include_sns_arn=True):
        """Fills in the bucket name entries in the config object.

        If we're server-side, we need to fill in [S3_BUCKET_DATABASE], [S3_BUCKET_TRUSTED], and [S3_BUCKET_EXPORT].
        These can be found in the ivert_setup/setup/paths.sh file from the ivert_setup repository."""
        assert hasattr(self, "s3_bucket_database")
        assert hasattr(self, "s3_bucket_import_untrusted")
        assert hasattr(self, "s3_bucket_import_trusted")
        assert hasattr(self, "s3_bucket_export")
        if include_sns_arn:
            assert hasattr(self, "sns_topic_arn")

        if not os.path.exists(self.ivert_setup_paths_file):
            raise FileNotFoundError(f"ivert_setup_paths_file not found: {self.ivert_setup_paths_file}")

        with open(self.ivert_setup_paths_file, 'r') as f:
            paths_text_lines = [line.strip() for line in f.readlines()]

        # Get the S3 bucket names from the paths.sh file
        # For each variable, look for the line that starts with it, extract the value after the =, and strip off any comments.

        try:
            db_line = [line for line in paths_text_lines if line.lower().startswith("s3_bucket_database")][0]
            self.s3_bucket_database = db_line.split("=")[1].split("#")[0].strip().strip("'").strip('"')
        except IndexError:
            self.s3_bucket_database = None

        try:
            trusted_line = [line for line in paths_text_lines if line.lower().startswith("s3_bucket_import_trusted")][0]
            self.s3_bucket_import_trusted = trusted_line.split("=")[1].split("#")[0].strip().strip("'").strip('"')
        except IndexError:
            self.s3_bucket_import_trusted = None
        try:
            untrusted_line = [line for line in paths_text_lines if line.lower().startswith("s3_bucket_import_untrusted")][0]
            self.s3_bucket_import_untrusted = untrusted_line.split("=")[1].split("#")[0].strip().strip("'").strip('"')
        except IndexError:
            self.s3_bucket_import_untrusted = None
        try:
            export_line = [line for line in paths_text_lines if line.lower().startswith("s3_bucket_export")][0]
            self.s3_bucket_export = export_line.split("=")[1].split("#")[0].strip().strip("'").strip('"')
        except IndexError:
            self.s3_bucket_export = None

        if include_sns_arn:
            try:
                sns_line = [line for line in paths_text_lines if line.lower().startswith("cudem_sns_arn")][0]
                self.sns_topic_arn = sns_line.split("=")[1].split("#")[0].strip().strip("'").strip('"')
            except IndexError:
                self.sns_topic_arn = None

        # Check to see if any of these just reference other variables. If so, fill them in. This could just point
        # to another variable, so keep looping until we've gotten an actual value.
        for varname in ["s3_bucket_database", "s3_bucket_import_untrusted", "s3_bucket_import_trusted", "s3_bucket_export"]:
            if getattr(self, varname) is None:
                continue

            # Since we're reading from a bash shell script, variables are defined as $varname.
            while getattr(self, varname).find("$") > -1:
                varname_from = getattr(self, varname).replace("$", "")
                setattr(self, varname, getattr(self, varname_from))

        return


    def _add_user_variables_and_s3_creds_to_config_obj(self):
        """Add the names of the S3 buckets to the configfile.config object.

        On a client instance, src setup needs to be run to flesh out the user configfile, before this will work."""
        # Make sure all these are defined in here. They may be assigned to None but they should exist. This is
        # a sanity check in case we changed the bucket variables names in the configfile.
        assert hasattr(self, "s3_bucket_import_untrusted")
        assert hasattr(self, "s3_bucket_export")
        assert hasattr(self, "user_email")
        assert hasattr(self, "username")
        assert hasattr(self, "aws_profile_ivert_ingest")
        assert hasattr(self, "aws_profile_ivert_export")

        # If we're on the server side (in the AWS), get these from the "ivert_setup" repository under /setup/paths.sh.
        #    In this case, only the s3_bucket_import_trusted, s3_bucket_database, and s3_bucket_export are needed.
        if self.is_aws:
            self._fill_bucket_names_from_ivert_setup()

        # If we're on the client side (not in an AWS instance), get these from the user configfile.
        else:
            if os.path.exists(self.user_configfile):
                user_config = config(self.user_configfile)
                self.user_email = user_config.user_email
                self.username = user_config.username
                self.aws_profile_ivert_ingest = user_config.aws_profile_ivert_ingest
                self.aws_profile_ivert_export = user_config.aws_profile_ivert_export
            else:
                pass

            # Now try to read the s3 credentials file.
            if not os.path.exists(os.path.abspath(self.ivert_s3_credentials_file)):
                return
            else:
                s3_credentials = config(self.ivert_s3_credentials_file)
                self.s3_bucket_import_untrusted = s3_credentials.s3_untrusted_bucket_name
                self.s3_bucket_export = s3_credentials.s3_export_bucket_name

        return
