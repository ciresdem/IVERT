import os


def is_conda():
    "Return True if running in an Anaconda Python environment. False otherwise."

    # Check for the CONDA_DEFAULT_ENV environment variable, which should be set in any conda python environment.
    # Most non-conda environments don't use this.
    return 'CONDA_DEFAULT_ENV' in os.environ


if __name__ == "__main__":
    print(is_conda())
