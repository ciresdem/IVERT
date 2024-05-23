#!/usr/bin/env python
"""ivert_client.py -- The front-facing interfact to IVERT code for cloud computing."""
import argparse
import os

import new_user_setup
import ivert_client_subscriptions

def define_and_parse_args(return_parser: bool = False):
    parser = argparse.ArgumentParser(description="The ICESat-2 Validation of Elevations Reporting Tool (IVERT)")

    # The first argument in the command. The other sub-parsers depend upon which command was used here.
    subparsers = parser.add_subparsers(dest="command",
                                       help=f"Run '{os.path.basename(__file__)} <command> --help' for more info about each command.")
    subparsers.required = True

    ###############################################################
    # Create the "validate" subparser
    ###############################################################
    validate_help_msg = "Validate DEMs using IVERT (the core functionality)."
    parser_validate = subparsers.add_parser("validate", help=validate_help_msg, description=validate_help_msg)
    parser_validate.add_argument("files_or_directory", type=str, nargs="+",
                                 help="Enter a file, list of files, or a directory."
                                      " May use bash-style wildcards such as ncei*.tif")
    parser_validate.add_argument("-ivd", "--input_vdatum", dest="input_vdatum", type=str, default="egm2008",
                                 help="Input DEM vertical datum. (Default: 'egm2008')"
                                      " Other options are: [TODO: FILL IN SOON]")
    parser_validate.add_argument("-ovd", "--output_vdatum", dest="output_vdatum", type=str, default="egm2008",
                                 help="Output DEM vertical datum. (Default: 'egm2008')"
                                      " Other options are: [TODO: FILL IN SOON]")
    parser_validate.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                                 help="Wait to exit until the results are finished and downloaded. Default:"
                                      " just upload the data and exit. You can then run 'ivert_client.py check <job_id>' to check the status"
                                      " of the job and 'ivert_client.py download <job_id> --local_dir <dirname>' to download results."
                                      " Default: False")
    parser_validate.add_argument("-d", "--dry_run", dest="dry_run", default=False, action="store_true",
                                 help="Print out the complete config options that will be used for this job and then"
                                      " exit. Do not upload files or submit the job to IVERT.")
    parser_validate.add_argument("-p", "--prompt", dest="prompt", default=False, action="store_true",
                                 help="Prompt the user to verify settings before uploading files to IVERT. Default: False")
    # TODO: Parse the "files_or_directory" argument and replace it with a "files" argument listing all the files.

    ###############################################################
    # Create the "setup" subparser
    ###############################################################
    setup_help_msg = ("Install the IVERT client on the local machine and create user settings. "
                      "Run this once before using IVERT. Re-run again to change settings."
                      " Note: It is recommended to get the ivert_s3_credentials.ini file and put it the ~/.ivert/creds/ "
                      "directory. It will save you from having to copy-paste each credential from that file.")
    # Use the parent parser from new_user_setup.py to define the arguments for the subparser
    parser_setup = subparsers.add_parser("setup",
                                         parents=[new_user_setup.define_and_parse_args(just_return_parser=True)],
                                         add_help=False,
                                         help=setup_help_msg, description=setup_help_msg)

    ###############################################################
    # Create the "test" subparser
    ###############################################################
    test_help_msg = "Test the end-to-end functionalty of IVERT in the NCCF system." + \
                    " No data will be processed, but a test file will be pushed and a textfile" + \
                    " will be received on the other end. This will test your connectivity with the" + \
                    " IVERT system."
    parser_test = subparsers.add_parser("test", help=test_help_msg, description=test_help_msg)
    parser_test.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                             help="Wait to exit until the results are finished and downloaded. If False,"
                                  " just upload the data and exit. You can run 'ivert_client.py check <job_id>' to check the status"
                                  " of the job and 'ivert_client.py download <job_id> --local_dir <dirname>' to download results."
                                  " Default: False")
    parser_test.add_argument("-p", "--prompt", dest="prompt", default=False, action="store_true",
                             help="Prompt the user to verify settings before uploading files to IVERT. Default: False")

    ###############################################################
    # Create the "status" subparser
    ###############################################################
    status_help_msg = "Check the status of an IVERT job."
    parser_status = subparsers.add_parser("status", help=status_help_msg, description=status_help_msg)
    parser_status.add_argument("job_id", type=str, default="LATEST",
                               help="Enter the job ID to check. Typically in a '<user.name>_<number>' format."
                                    " Default: Check the latest job submitted by this user fromn this machine.")
    parser_status.add_argument("-d", "--download_if_finished", dest="download_if_finished",
                               default=False, action="store_true",
                               help="Automatically download results if the job has finished. Default: False")
    parser_status.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                               help="Wait to exit until the results are finished, then downloaded them."
                                    " Default: False (return status and exit immediately).")
    parser_status.add_argument("-ld", "--local_dir", dest="local_dir", type=str, default=".",
                               help="Specify the local directory to download results. Default: '.'")

    ###############################################################
    # Create the "download" subparser
    ###############################################################
    download_help_msg = "Download the results of an IVERT job."
    parser_download = subparsers.add_parser("download", help=download_help_msg, description=download_help_msg)
    parser_download.add_argument("job_id", type=str, default="LATEST",
                                 help="Enter the job ID to download, typically a 12-digit number in YYYYMMDDNNNN format."
                                      " Default: Download the latest job submitted by this user.")
    parser_download.add_argument("-u", "--user", "--username", dest="username", type=str, default="",
                                 help="Manually specify the IVERT username. Default: Use the username of the current "
                                      "user saved in ~/.ivert/creds/ivert_user_config.ini.")
    parser_download.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                                 help="Wait to exit until the results are finished, then downloaded them. "
                                      "Default: Print the job status and exit immediately.")
    parser_download.add_argument("-ld", "--local_dir", dest="local_dir", type=str, default=".",
                                 help="Specify the local directory to download results. Default: '.'")

    ###############################################################
    # Create the "update" subparser
    ###############################################################
    update_help_msg = "Request updated data in the IVERT photon database."
    parser_update = subparsers.add_parser("update", help=update_help_msg, description=update_help_msg)
    parser_update.add_argument("polygon_file", type=str,
                               help="Enter a polygon file (.shp, .json, .geojson, or .gkpg).")
    parser_update.add_argument( "-s", "--start_date", dest="start_date", type=str, default="1 year ago",
                               help="Any date string readable by python dateparser."
                                    " See https://dateparser.readthedocs.io/en/latest/ for details."
                                    " Default: '1 year ago'.")
    parser_update.add_argument("-e", "--end_date", dest="end_date", type=str, default="midnight today",
                               help="Any date string readable by python dateparser."
                                    " See https://dateparser.readthedocs.io/en/latest/ for details."
                                    " end_date must be later than start_date."
                                    " Default: 'midnight today'.")
    parser_update.add_argument("-sbgc", "--skip_bad_granule_checks", dest="skip_bad_granule_checks",
                               default=False, action="store_true",
                               help="Skip post-processing to check for bad granules."
                                    " Default: False (post-process to elimiate bad granules).")
    parser_update.add_argument("-l", "--leave_old_data", dest="leave_old_data",
                               default=False, action="store_true",
                               help="Leave old data in the database after new data has been written and checked. WARNING:"
                                    " This may results in data redundancy if new data periods overlap existing data periods. Old"
                                    " records should be removed manually sometime after this operation."
                                    " Default: False (delete old data after writing new data.")
    parser_update.add_argument("-p", "--prompt", default=False, action="store_true",
                               help="Prompt the user to verify settings before uploading files to IVERT. Default: False")

    ###############################################################
    # Create the "import" subparser
    ###############################################################
    import_help_msg = "Import data into the IVERT tool."
    parser_import = subparsers.add_parser("import", help=import_help_msg, description=import_help_msg)
    parser_import.add_argument("files", type=str, nargs="+",
                               help="Enter a file, list of files, or a directory to import into the IVERT work bucket."
                                    " May use bash-style wildcards such as ivert*.feather.")
    parser_import.add_argument("-d", "-dest", "--destination_prefix", dest="destination_prefix",
                               type=str, default="",
                               help="Destintion prefix to place files into the IVERT work bucket."
                                    " Default: '', for the base directory.")
    parser_import.add_argument("-p", "--prompt", default=False, action="store_true",
                               help="Prompt the user to verify settings before uploading files to IVERT. Default: False")

    ###############################################################
    # Create the "subscribe" subparser
    ###############################################################
    subscribe_help_msg = ("Subscribe to IVERT email notifications. It will overwrite any previous subscriptions defined"
                          " for that user.")
    parser_subscribe = subparsers.add_parser("subscribe", help=subscribe_help_msg, description=subscribe_help_msg)
    parser_subscribe.add_argument("email", type=str,
                                  help="Enter an email address to subscribe to IVERT email notifications.")
    parser_subscribe.add_argument("-a", "--all", dest="all", default=False, action="store_true",
                                  help="Subscribe to all IVERT email notifications. Default: Only get notified for jobs coming from your username.")
    parser_subscribe.add_argument("-u", "--username", dest="username", type=str, default=None,
                                  help="The username of the IVERT user upon which to filter the sns notificaions, if different from the default. Default: Username is derived from your email (before the '@' symbol). You usually shouldn't need this option. Ignored if --all is set.")

    ###############################################################
    # Create the "unsubscribe" subparser
    ###############################################################
    unsubscribe_help_msg = "Unsubscribe from IVERT email notifications. This can also be done by using the 'unsubscribe' link in any IVERT emails you receive."
    parser_unsubscribe = subparsers.add_parser("unsubscribe", help=unsubscribe_help_msg, description=unsubscribe_help_msg)
    parser_unsubscribe.add_argument("email", type=str,
                                    help="Enter an email address to unsubscribe from IVERT email notifications.")

    if return_parser:
        return parser
    else:
        return parser.parse_args()


def ivert_client_cli():
    """Run the IVERT client CLI."""
    args = define_and_parse_args()

    # Set up the IVERT client on a new system
    if args.command == "setup":
        new_user_setup.setup_new_user(args)

    # Subscribe to IVERT email notifications
    elif args.command == "subscribe":
        ivert_client_subscriptions.run_subscribe_command(args)

    # Unsubscribe from IVERT email notifications
    elif args.command == "unsubscribe":
        ivert_client_subscriptions.run_unsubscribe_command(args)

    # Validate a set of DEMs
    elif args.command == "validate":
        # TODO: Implement this
        raise NotImplementedError("Command 'validate' not yet implemented.")
        pass

    # Download results from IVERT
    elif args.command == "download":
        # TODO: Implement this
        raise NotImplementedError("Command 'download' not yet implemented.")
        pass

    # Update part of the IVERT database.
    elif args.command == "update":
        # TODO: Implement this
        raise NotImplementedError("Command 'update' not yet implemented.")
        pass

    # Test the IVERT client and server in an end-to-end "dry run."
    elif args.command == "test":
        # TODO: Implement this
        raise NotImplementedError("Command 'test' not yet implemented.")
        pass

    # Check on the status of a running job
    elif args.command == "status":
        # TODO: Implement this
        raise NotImplementedError("Command 'status' not yet implemented.")
        pass

    # Import data into the IVERT tool (for setup purposes only)
    elif args.command == "import":
        # TODO: Implement this
        raise NotImplementedError("Command 'import' not yet implemented.")
        pass

    # Raise an error if the command doesn't exist.
    else:
        raise NotImplementedError("Command '{args.command}' does not exist in IVERT or is not implemented.")


if __name__ == "__main__":
    ivert_client_cli()