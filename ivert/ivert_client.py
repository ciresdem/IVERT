#!/usr/bin/env python
"""ivert_client.py -- The front-facing interfact to IVERT code for cloud computing."""
import argparse
import os

import new_user_setup
import client_subscriptions
import client_job_download
import client_test_job
import client_job_status
import utils.query_yes_no as yes_no

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
    # NOTE: The script client_test_job.py creates an identical copy of this argument list to send off a test job.
    # If any of these options are changed, go change the equivalent lines in that script as well to match the same
    # field names.
    parser_validate = subparsers.add_parser("validate", help=validate_help_msg, description=validate_help_msg)
    parser_validate.add_argument("files_or_directory", type=str, nargs="+",
                                 help="Enter a file, list of files, or a directory. "
                                      "May use bash-style wildcards such as 'dirname/ncei*.tif'. If a directory is "
                                      "given, all *.tif files in that directory will be sent for validation.")
    parser_validate.add_argument("-ivd", "--input_vdatum", dest="input_vdatum", type=str, default="egm2008",
                                 help="Input DEM vertical datum. (Default: 'egm2008')"
                                      " Type 'vdatums -list-epsg' to see a list of available options.")
    parser_validate.add_argument("-ovd", "--output_vdatum", dest="output_vdatum", type=str, default="egm2008",
                                 help="Output DEM vertical datum. (Default: 'egm2008')"
                                      " Type 'vdatums -list-epsg' to see a list of available options.")
    parser_validate.add_argument("-n", "--name", "--region_name", dest="region_name", type=str, default="DEMs",
                                 help="The name of the region being validated. Will appear in the validation summary "
                                      "plot if more than one file is being validated. (Default: 'DEMs')")
    parser_validate.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                                 help="Wait to exit until the results are finished and downloaded. If False, just "
                                      "upload the job, exit, and wait for a response notification from IVERT. You can "
                                      "then use the 'ivert status' and 'ivert download' commands to monitor the job. "
                                      "Default: False")
    parser_validate.add_argument("-p", "--prompt", dest="prompt", default=False, action="store_true",
                                 help="Print the command options and prompt the user to verify settings before uploading"
                                      " files to IVERT. Useful if you want to manually double-check the settings"
                                      " before sending it off. Default: False")
    parser_validate.add_argument("-mc", "--measure_coverage", dest="measure_coverage",
                                 default=False, action="store_true",
                                 help="Measure the coverage of the region as a field in the h5 results. "
                                      "(Measures how may of the 225 sub-regions within each grid cell contain photons, "
                                      "allowing to post-process filter only higher-coverage grid cells in "
                                      "course-resolution DEMs. Typically not used for high-res DEMs. Default: False")
    parser_validate.add_argument("-bn", "--band_num", dest="band_num", type=int, default=1,
                                 help="The raster band number to validate. Other bands are ignored. (Default: 1)")
    parser_validate.add_argument("-co", "--coastlines_only", dest="coastlines_only", default=False,
                                 action="store_true",
                                 help="Return only the coastline masks. Skip the rest of the validation. Default: False")
    parser_validate.add_argument("-mb", "--mask_buildings", dest="buildings", type=yes_no.interpret_yes_no,
                                 default=True,
                                 help="Whether to mask out building footprints in the coastline mask. Must be followed "
                                      "by 'True', 'False', 'Yes', 'No', or any abbreviation thereof (case-insensitive). "
                                      "(Default: True)")
    parser_validate.add_argument("-mu", "--mask_urban", dest="urban", type=yes_no.interpret_yes_no,
                                 default=False,
                                 help="Whether to mask out World-Settlement-Footprint heavy urban areas in the "
                                      "coastline mask. Typically used instead of building footprints for DEMs coarser"
                                      "than typical building sizes (~20-ish m). Must be followed by 'True', 'False', "
                                      "'Yes', 'No', or any abbreviation thereof (case-insensitive). (Default: False)")
    parser_validate.add_argument("-sd", "--outlier_sd_threshold", dest="outlier_sd_threshold", type=float,
                                 default=2.5,
                                 help="The standard deviation threshold for outlier detection. Any errors "
                                      "outside this threshold of the mean-of-errors will be removed as noise. "
                                      "-1 (or any negative number) will disable outlier filtering. Don't use 0 here, "
                                      "that'd filter everything out. (Default: 2.5 s.d.)")
    # TODO: Parse the "files_or_directory" argument and replace it with a "files" argument listing all the files.

    ###############################################################
    # Create the "setup" subparser
    ###############################################################
    setup_help_msg = ("Install the IVERT client on the local machine and create user settings. "
                      "Run this once before using IVERT. Re-run again to change settings."
                      " Note: It is recommended to get the ivert_s3_credentials.ini file and put it the ~/.ivert/creds/ "
                      "directory. It will save you from having to copy-paste each credential from that file.")
    # Use the parent parser from new_user_setup.py to define the arguments for the subparser
    subparsers.add_parser("setup",
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

    ###############################################################
    # Create the "status" subparser
    ###############################################################
    status_help_msg = "Check the status of an IVERT job."
    parser_status = subparsers.add_parser("status", help=status_help_msg, description=status_help_msg)
    parser_status.add_argument("job_name", type=str, nargs='?', default="LATEST",
                               help="Enter the job name to check. Typically in a '<user.name>_<12-digit-number>' format."
                                    " Default: Check the latest job submitted by this user.")
    parser_status.add_argument("-d", "--detailed", dest="detailed", default=False, action="store_true",
                               help="Give detailed information about the current status of the job and all its files."
                                    " Default: Just give the overall job status.")

    ###############################################################
    # Create the "download" subparser
    ###############################################################
    download_help_msg = "Download the results of an IVERT job."
    parser_download = subparsers.add_parser("download", help=download_help_msg, description=download_help_msg)
    parser_download.add_argument("job_id_or_name", type=str, nargs='?', default="LATEST",
                                 help="Enter the job ID to download, typically a 12-digit number in YYYYMMDDNNNN"
                                      " format or a 'username_YYYYMMMDDNNNN' format. Either one is valid. If the"
                                      " username isn't given, it will be looked up from the user_config.ini file on"
                                      " this machine. Default: Downloads the latest job submitted by this user.")
    parser_download.add_argument("-o", "--output_dir", dest="output_dir", type=str, default=".",
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

    # Not implemented yet.
    # ###############################################################
    # # Create the 'kill' subparser
    # ###############################################################
    # kill_help_msg = "Terminate an IVERT job."
    # parser_kill = subparsers.add_parser("kill", help=kill_help_msg, description=kill_help_msg)
    # parser_download.add_argument("job_id", type=str, default="LATEST",
    #                              help="Enter the job ID to download, typically a 12-digit number in YYYYMMDDNNNN format."
    #                                   " Default: Download the latest job submitted by this user.")
    # parser_download.add_argument("-u", "--user", "--username", dest="username", type=str, default="",
    #                              help="Manually specify the IVERT username. Default: Use the username of the current "
    #                                   "user saved in ~/.ivert/creds/ivert_user_config.ini.")

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
        client_subscriptions.run_subscribe_command(args)

    # Unsubscribe from IVERT email notifications
    elif args.command == "unsubscribe":
        client_subscriptions.run_unsubscribe_command(args)

    # Validate a set of DEMs
    elif args.command == "validate":
        # TODO: Implement this
        raise NotImplementedError("Command 'validate' not yet implemented.")
        pass

    # Download results from IVERT
    elif args.command == "download":
        client_job_download.run_download_command(args)

    # Update part of the IVERT database.
    elif args.command == "update":
        # TODO: Implement this
        raise NotImplementedError("Command 'update' not yet implemented.")
        pass

    # Test the IVERT client and server in an end-to-end "test run."
    elif args.command == "test":
        client_test_job.run_test_command(args)

    # Check on the status of a running job
    elif args.command == "status":
        client_job_status.run_job_status_command(args)

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