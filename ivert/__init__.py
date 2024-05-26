try:
    from version import __version__
except ModuleNotFoundError:
    from ivert.version import __version__