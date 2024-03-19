"""ivert.py -- The front-facing interfact to IVERT code for cloud computing."""


import argparse

##########################################################
# TODO 1: Code for reading input configuration files.

# TODO 2: Code for finding the correct "new job number" and mapping the new job to the "Untrusted" input bucket.
# This will require access to data from the EC2. Look to Tom's guidance for IAM credentials to our work bucket.

# TODO 3: Code to generate a "submission-config" file profile to send to the untrusted bucket.

# TODO 4: Code to upload the files and "submit" the new job.

def define_and_parse_args():
    parser = argparse.ArgumentParser(description="Submit a job to the ICESat-2 Validation of Elevations Reporting Tool (IVERT)"
                                     " for processing in the cloud.")

    # TODO: Fill out the rest of the command-line arguments here.

    return parser.parse_args()

if __name__ == "__main__":
    args = define_and_parse_args()

    # TODO: Fill in command-line arguments here.