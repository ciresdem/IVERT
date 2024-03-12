# -*- coding: utf-8 -*-
import ast
import configparser
# If I import this script from a script in the parent directory, the "import is_aws" breaks. Instead, use "import utils/is_aws"
try:
    import is_aws
except ModuleNotFoundError:
    import utils.is_aws
import os
import re
import sys
# import cryptography.fernet
# import getpass
# import socket
# import base64


class config:
    """A subclass implementation of configparser.ConfigParser(), expect that config attributes are referenced as object
    attributes rather than in a dictionary.

    So if the .ini file contains the attribute:
         varname = 0
    it is referenced by:
         c = configfile.config()
         c.varname
         >> 0

    When initialized, it will check whether or not it is running in an AWS (Amazon Web Services) cloud environment
    and if so, use the [AWS] section of the configfile.

    The two sections in the .ini configfile should be [DEFAULT] and [AWS].
    No other sections are ready by this object, for now.
    """

    def __init__(self,
                 configfile=os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "ivert_config.ini"))):

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
            if sys.platform in ('linux', 'cygwin', 'darwin') and value.find("/") > -1:
                # If it's an absolute path or it already exists where it is, just use it as-is
                if value.strip().find("/") == 0:
                    setattr(self, key, os.path.abspath(value))
                # If it's a relative path, make it relative to the _configfile's local directory.
                else:
                    # If this points to an S3-bucket key-path, don't return an absolute path, just keep it as it is.
                    # if key[:2].lower() == "s3":
                    #     setattr(self, key, value)
                    # else:
                    setattr(self, key, self._abspath(os.path.join(os.path.dirname(self._configfile), value)))
                return

            elif sys.platform in ('win32', 'win64') and value.find("\\") > -1:
                # If it's an absolute path or it already exists where it is, just use it as-is
                # For a base path, look for the "C:\" drive-name pattern at the start (upper- or lower-case).
                if re.search(r'\A[A-Za-z]\:\\', value.strip()) is not None:
                    setattr(self, key, os.path.abspath(value))
                # If it's a relative path, make it relative to the _configfile directory.
                else:
                    # if key[:2].lower() == "s3":
                    #     setattr(self, key, value)
                    # else:
                    setattr(self, key, self._abspath(os.path.join(os.path.dirname(self._configfile), value)))
                return
        except ValueError:
            pass

        # Otherwise, it's probably just a string value, set it as-is.
        setattr(self, key, value)
        return

# FOR IVERT, I am currently eliminating the functionality of this class to save and retrieve account credentials.
# To use the NSIDC module, simply copy a "username password" combo for NASA EarthAccess into the local ~/.netrc file.
# It should work from there.

    # def _save_credentials(self, username,
    #                             password,
    #                             fname_attribute="nsidc_cred_file",
    #                             warn_if_exists=True,
    #                             verbose=False):
    #     """Save an encrypted password file.
    #
    #     Use the config attribute stored in self.[value of attr_fname] for the filename.
    #         Defaults to self.nsidc_creds_file, but could change the attribute name
    #
    #     NOTE: This is not considered a secure encryption. It scrambles and obfuscates the
    #     NSIDC credentials so that it is not saved plain-text locally, but
    #     a motivated hacker could read this function, figure out how to decrypt
    #     the file (if they know your system's computer and local username you use), and
    #     and retrieve the NSIDC credentials. This is only as secure as the user's
    #     local filesystem. If you need to keep the NSIDC credential truly secret,
    #     we suggest you re-enter them manually upon each use and do not try to
    #     store them locally at all.
    #     """
    #     cred_fname = getattr(self, fname_attribute)
    #
    #     # Check if we should overwrite the file.
    #     overwrite_file = False
    #     if os.path.exists(cred_fname):
    #         # Prompt the user if they want to overwrite their old credentials.
    #         if warn_if_exists:
    #             response = None
    #             while not response:
    #                 response = input("File '{0}' already exists. Overwrite? [Y/n] "
    #                                  .format(os.path.split(cred_fname)[1]))
    #                 response = response.strip().upper()
    #                 if len(response) == 0 or response[0] == "Y":
    #                     overwrite_file = True
    #                 elif response[0] == "N":
    #                     overwrite_file = False
    #                 else:
    #                     response = None
    #         else:
    #             overwrite_file = True
    #
    #         if overwrite_file:
    #             os.remove(cred_fname)
    #
    #     # Get the encryption object for this machine.
    #     fernet = self._get_fernet()
    #
    #     # Encrypt that stuff and put it in a local file.
    #     username_colon_password = (username + ":" + password).encode("utf-8")
    #     enc_text = fernet.encrypt(username_colon_password)
    #
    #     with open(cred_fname, 'wb') as f:
    #         f.write(enc_text)
    #         f.close()
    #
    #         if verbose:
    #             print("NSIDC credentials have been encrypted and saved in '{}'.".format(cred_fname))
    #
    #     return
    #
    # def _read_credentials(self, fname_attribute="nsidc_cred_file", verbose=True):
    #     """Get the username/password from the text-obfuscated cred file.
    #
    #     Returns: Username, password
    #     Note: Must be excecuted by the same user, logged onto the same workstation,
    #     as when the scrambled file was generated. Otherwise, it will not be able
    #     to decrypt the file.
    #
    #     If the file does not exist, or if it cannot be decrypted, returns (None, None).
    #     """
    #
    #     cred_fname = getattr(self, fname_attribute)
    #
    #     # If the path doesn't exist, return None
    #     if not os.path.exists(cred_fname):
    #         return (None, None)
    #
    #     fernet = self._get_fernet()
    #
    #     with open(cred_fname, 'rb') as f:
    #         enc_text = f.read()
    #         f.close()
    #
    #     try:
    #         username_colon_password = fernet.decrypt(enc_text).decode("utf-8")
    #     except cryptography.fernet.InvalidToken:
    #         if verbose:
    #             print("Unable to decrypt file '{0}'. Credentials will need to be re-entered.".format(cred_fname))
    #         return (None, None)
    #
    #     colon_pos = username_colon_password.find(":")
    #
    #     username = username_colon_password[:colon_pos]
    #     pwd = username_colon_password[(colon_pos+1):]
    #
    #     return username, pwd
    #
    # def _remove_credentials(self, fname_attribute="nsidc_cred_file", verbose=True):
    #     """Remove the credential file."""
    #     cred_fname = getattr(self, fname_attribute)
    #     if os.path.exists(cred_fname):
    #
    #         os.remove(cred_fname)
    #
    #         if verbose:
    #             "Deleting credential file {}.".format(cred_fname)
    #
    # def _get_fernet(self):
    #     salt_key = self._get_salt()
    #     # print(len(salt_key), salt_key, repr(salt_key))
    #     return cryptography.fernet.Fernet(salt_key)
    #
    # def _get_salt(self):
    #     """Get a salt value for the hash algorithm.
    #
    #     For now, use computer name + system username + a random filler, all cropped at 32 chars.
    #     This should be the same each time someone logs into the same machine, and will not work on any other machine
    #     nor with any other username."""
    #     filler_str = "random_stuff_that_is_at_least_32_bytes_long_or_so_to_use_as_filler_well_lets_see_here"
    #     return base64.b64encode((getpass.getuser() + socket.gethostname() + filler_str)[0:32].encode())
