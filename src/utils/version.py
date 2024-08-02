import os
import sys

if vars(sys.modules[__name__])['__package__'] == 'ivert_utils':
    # When this is built a setup.py package, it names the modules 'ivert' and 'ivert_utils'. This reflects that.
    import ivert_utils.version_check_client as version_check_client
    import ivert_utils.version_check_server as version_check_server
else:
    try:
        # If running as a script, import this way.
        import version_check_client
        import version_check_server
    except ModuleNotFoundError:
        # If this script is imported from another module in the src/ directory, import this way.
        import utils.version_check_client as version_check_client
        import utils.version_check_server as version_check_server

__version__ = None
__minimum_client_version__ = None


def current_version():
    # If the version has already been read from the file, just return it.
    global __version__
    if __version__ is not None:
        return __version__

    # in the codes files.
    VFILE1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "VERSION"))
    # In the package files, after installing.
    VFILE2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ivert_data", "VERSION"))

    if vars(sys.modules[__name__])['__package__'] == 'ivert_utils' and os.path.exists(VFILE2):
        file_to_use = VFILE2
    elif os.path.exists(VFILE1):
        file_to_use = VFILE1
    else:
        raise FileNotFoundError("Could not find 'VERSION' file.")

    with open(file_to_use, 'r') as fobj:
        v = fobj.read().strip()
        return v


# Define the __version__ variable in this module.
__version__ = current_version()


# If we're in the ivert client package, fetch the minimum client version from the server.
if vars(sys.modules[__name__])['__package__'] == 'ivert_utils':
    __minimum_client_version__ = version_check_client.fetch_min_client_from_server()
# Otherwise fetch it locally.
else:
    __minimum_client_version__ = version_check_server.minimum_client_version()


if __name__ == "__main__":
    print(__version__)
