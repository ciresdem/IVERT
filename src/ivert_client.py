#!/usr/bin/env python
"""ivert.py -- The front-facing interfact to IVERT code for cloud computing."""
import argparse
import os

import ivert_new_user_setup

##########################################################
# TODO 1: Code for reading input configuration files.

# TODO 2: Code for finding the correct "new job number" and mapping the new job to the "Untrusted" input bucket.
# This will require access to data from the EC2. Look to Tom's guidance for IAM credentials to our work bucket.

# TODO 3: Code to generate a "submission-config" file profile to send to the untrusted bucket.

# TODO 4: Code to upload the files and "submit" the new job.

def define_and_parse_args():
    parser = argparse.ArgumentParser(description="The ICESat-2 Validation of Elevations Reporting Tool (IVERT)")

    # The first argument in the command. The other sub-parsers depend upon which command was used here.
    subparsers = parser.add_subparsers(dest="command",
                                       help=f"Run '{os.path.basename(__file__)}' <command> --help' for more info about each command.")
    subparsers.required = True

    # Create the "validate" subparser
    validate_help_msg = "Validate DEMs using IVERT (the core functionality)."
    parser_validate = subparsers.add_parser("validate", help=validate_help_msg, description=validate_help_msg)
    parser_validate.add_argument("files_or_directory", type=str, nargs="+",
                                 help="Enter a file, list of files, or a directory."
                                      " Can also enter a bash-style wildcard such as ncei*.tif")
    parser_validate.add_argument("-ivd", "--input_vdatum", type=str, default="egm2008",
                                 help="Input DEM vertical datum. (Default: 'egm2008')"
                                 " Other options are: [TODO: FILL IN SOON]")
    parser_validate.add_argument("-ovd", "--output_vdatum", type=str, default="egm2008",
                                 help="Output DEM vertical datum. (Default: 'egm2008')"
                                 " Other options are: [TODO: FILL IN SOON]")
    parser_validate.add_argument("-w", "--wait", default=False, action="store_true",
                                 help="Wait to exit until the results are finished and downloaded. Default:"
                                      " just upload the data and exit. You can then run 'ivert.py check <job_id>' to check the status"
                                      " of the job and 'ivert.py download <job_id> --local_dir <dirname>' to download results."
                                      " Default: False")
    parser_validate.add_argument("-d", "--dry_run", default=False, action="store_true",
                                 help="Print out the complete config options that will be used for this job and then"
                                      " exit. Do not upload files or submit the job to IVERT.")

    # Create the "setup" subparser
    setup_help_msg = "Set up a new IVERT user on the local machine. Run once before using IVERT. Re-run again to change settings."
    # Use the parent parser from ivert_new_user_setup.py to define the arguments for the subparser
    setup_parser = subparsers.add_parser("setup",
                                         parents=[ivert_new_user_setup.define_and_parse_args(just_return_parser=True)],
                                         add_help=False,
                                         help=setup_help_msg, description=setup_help_msg)


    # Create the "test" subparser
    test_help_msg = "Test the end-to-end functionalty of IVERT in the NCCF system." + \
                    " No data will be processed, but a test file will be pushed and a textfile" + \
                    " will be received on the other end. This will test your connectivity with the" + \
                    " IVERT system."
    parser_help = subparsers.add_parser("test", help=test_help_msg, description=test_help_msg)
    parser_help.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                             help="Wait to exit until the results are finished and downloaded. If False,"
                                  " just upload the data and exit. You can run 'ivert.py check <job_id>' to check the status"
                                  " of the job and 'ivert.py download <job_id> --local_dir <dirname>' to download results."
                                  " Default: False")

    # Create the "check" subparser
    check_help_msg = "Check the status of an IVERT job."
    parser_check = subparsers.add_parser("check", help=check_help_msg, description=check_help_msg)
    parser_check.add_argument("job_id", type=str, default="LATEST",
                              help="Enter the job ID to check. Typically in a '<user.name>_<number>' format."
                                   " Default: Check the latest job submitted by this user.")
    parser_check.add_argument("-d", "--download_if_finished", dest="download_if_finished",
                              default=False, action="store_true",
                              help="Automatically download results if the job has finished. Default: False")
    parser_check.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                              help="Wait to exit until the results are finished, then downloaded them."
                                   " Default: False (return status and exit immediately).")
    parser_check.add_argument("-ld", "--local_dir", dest="local_dir", type=str, default=".",
                              help="Specify the local directory to download results. Default: '.'")

    # Create the "download" subparser
    download_help_msg = "Download the results of an IVERT job."
    parser_download = subparsers.add_parser("download", help=download_help_msg, description=download_help_msg)
    parser_download.add_argument("job_id", type=str, default="LATEST",
                                 help="Enter the job ID to download. Typically in a '<user.name>_<number>' format."
                                      " Default: Download the latest job submitted by this user.")
    parser_download.add_argument("-w", "--wait", dest="wait", default=False, action="store_true",
                                 help="Wait to exit until the results are finished, then downloaded them.")
    parser_download.add_argument("-ld", "--local_dir", dest="local_dir", type=str, default=".",
                                 help="Specify the local directory to download results. Default: '.'")

    # Create the "update" subparser
    update_help_msg = "Update the IVERT photon database."
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

    # Create the "import" subparser
    import_help_msg = "Import data into IVERT."
    parser_import = subparsers.add_parser("import", help=import_help_msg, description=import_help_msg)
    parser_import.add_argument("files", type=str,
                               help="Enter a file, list of files, or a directory to import into the IVERT work bucket."
                                    " Can also enter a bash-style wildcard such as *.feather.")
    parser_import.add_argument("-d", "-dest", "--destination_prefix", dest="destination_prefix",
                               type=str, default="",
                               help="Destintion prefix to place files into the IVERT work bucket."
                                    " Default: '', for the base directory.")

    return parser.parse_args()

if __name__ == "__main__":
    args = define_and_parse_args()

    if args.command == "setup":
        ivert_new_user_setup.setup_new_user(args)

    else:
        raise NotImplementedError("Command '{args.command}' not yet implemented.")