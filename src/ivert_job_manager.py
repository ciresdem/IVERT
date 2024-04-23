"""ivert_job_manager.py -- Code for managing and running IVERT jobs on an EC2 instance.

This should be always running, and there should only ever be ONE instance of it. When it kicks off it will test if
there are any other ivert_job_manager.py instances running. If so, it will exit.

Created by: Mike MacFerrin
April 22, 2024
"""

import argparse
import numpy
import os
import psutil
import time
import typing


def is_another_manager_running() -> typing.Union[bool, psutil.Process]:
    """Returns True if another instance of this script is already running.

    Returns:
        The psuil.Process object if another instance of this script is already running. False otherwise."""

    processes = psutil.process_iter()
    for p in processes:
        if "python" in p.name() and numpy.any([opt.endswith(os.path.basename(__file__)) for opt in p.cmdline()]):
            if p.pid != os.getpid():
                return p

    return False


class IvertJobManager:
    """Class for managing and running IVERT jobs on an EC2 instance.

    This will be initialized by the ivert_setup.sh script in the ivert_setup repository, using a supervisord process."""

    def __init__(self, time_interval_s: int = 100):
        """
        Initializes a new instance of the IvertJobManager class.

        Args:

        Returns:
            None
        """
        self.time_interval_s = time_interval_s
        pass

    def start(self):
        """Start the job manager.

        This is a persistent process and should be run in the background."""
        while True:
            time.sleep(self.time_interval_s)

            # TODO: Implement searching for new input files in the trusted bucket and kicking off new processes.

class IvertJob:
    """Class for managing and running IVERT individual jobs on an EC2 instance.

    The IvertJobManager class is always running and kicks off new IvertJob objects as needed."""

    def __init__(self,
                 job_config_s3_key: str,
                 job_config_s3_bucket_type: str = "trusted"):
        """
        Initializes a new instance of the IvertJob class.

        Args:
            job_config_s3_key (str): The S3 key of the job configuration file.
            job_config_s3_bucket_type (str): The S3 bucket type of the job configuration file. Defaults to "trusted".
        """


def define_and_parse_arguments() -> argparse.Namespace:
    """Defines and parses the command line arguments.

    Returns:
        An argparse.Namespace object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Ivert Job Manager... for detecting, running, and managing IVERT jobs on an EC2 instance.")
    parser.add_argument("-t", "--time_interval_s", type=int, target="time_interval_s", default=120,
                        help="The time interval in seconds between checking for new IVERT jobs. Default: 120.")

    return parser.parse_args()


if __name__ == "__main__":
    # If another instance of this script is already running, exit.
    other_manager = is_another_manager_running()
    if other_manager:
        print(f"Process {other_manager.pid}: '{other_manager.name()} {other_manager.cmdline()}' is already running.")
        exit(0)

    # Parse the command line arguments
    args = define_and_parse_arguments()

    # Start the job manager
    JM = IvertJobManager(time_interval_s=args.time_interval_s)
    JM.start()
