"""A script to run an end-to-end test job of the IVERT system, without having it actually process any data.

It creates and empty .tif file, uploads it to do a validation but uses the --empty-test option to do a dry run and return a log file."""

import argparse
import os

import utils.configfile
import utils.create_empty_tiff

def run_test_commend(args: argparse.Namespace) -> None:
    """Run an end-to-end test job of the IVERT system, without having it actually process any data."""

    ivert_config = utils.configfile.config()
    empty_tiff = ivert_config.empty_tiff

    if not os.path.exists(empty_tiff):
        utils.create_empty_tiff.create_empty_tiff()

