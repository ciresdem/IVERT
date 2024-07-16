"""Functions for managing files that have been quarantined when trying to input through the filtering system."""

import s3
import utils.configfile


def list_files_in_quarantine():
    """Return a list of all files that currently sit in quarantine."""
    iconfig = utils.configfile.config()
    s3m = s3.S3Manager()

    return s3m.listdir(iconfig.s3_quarantine_prefix_base +
                       ("" if iconfig.s3_quarantine_prefix_base[-1] == "/" else "/") + "*",
                       bucket_type='quarantine',
                       recursive=True)


def is_quarantined(s3_key: str) -> bool:
    """Return True/False if a file exists in quarantine."""
    iconfig = utils.configfile.config()
    s3m = s3.S3Manager()

    s3_key = s3_key.strip()
    if not s3_key.startswith(iconfig.s3_quarantine_prefix_base):
        s3_key = (iconfig.s3_quarantine_prefix_base + "/" + s3_key).replace("//", "/")

    return s3m.exists(s3_key,
                      bucket_type='quarantine')
