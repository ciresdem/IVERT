
import sys
import time
import multiprocessing as mp
import typing

if vars(sys.modules[__name__])['__package__'] == 'ivert_utils':
    # When this is built a setup.py package, it names the modules 'ivert' and 'ivert_utils'. This reflects that.
    import ivert_utils.unformat_text as ut
else:
    try:
        import unformat_text as ut
    except ModuleNotFoundError:
        # If this script is imported from another module in the src/ directory, import this way.
        import utils.unformat_text as ut


class Logger:
    def __init__(self, filename, include_terminal=True):
        if include_terminal:
            self.terminal = sys.stdout
        else:
            self.terminal = None

        self.log = open(filename, "a", buffering=1)

    def write(self, message):
        """Write a message to both the terminal and the log."""
        if self.terminal:
            self.terminal.write(message)

        self.log.write(self.strip_color_and_carriage_return(message))

    @staticmethod
    def strip_color_and_carriage_return(message):
        """Strip ANSI escape codes and lines with carriage returns."""
        return ut.unformat_and_delete_cr_lines(message)


def subproc_logger(target: callable,
                   filename: str,
                   args: typing.Union[typing.List, typing.Tuple, None] = None,
                   kwargs: typing.Union[typing.Dict, None] = None,
                   include_terminal: bool = False,
                   delay_s: typing.Union[int, float] = 0.01) -> mp.Process:
    """Kick off a multiprocessing process that logs stdout and sterr to a file.

    The logs are stripped of ANSI escape codes and lines with carriage returns.

    Args:
        target (callable): The function to call.
        filename (str): The filename to log to.
        args (list or tuple, optional): Arguments to pass to the function. Defaults to None.
        kwargs (dict, optional): Keyword arguments to pass to the function. Defaults to None.
        include_terminal (bool, optional): Whether to keep terminal output without unformatting. Defaults to False.
        delay_s (typing.Union[int, float], optional): Seconds to sleep before starting the process to avoid a race
                                                      condition with the process starting. Defaults to 0.01.

    Returns:
        multiprocessing.Process: The process that has been started.
    """
    # Capture the old stdout and stderr streams to reassign later.
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    # Reassign stdout and stderr to the Logger object.
    sys.stdout = Logger(filename, include_terminal=include_terminal)
    sys.stderr = sys.stdout

    # Start the process.
    proc = mp.Process(target=target, args=args, kwargs=kwargs)
    proc.start()

    # Sleep for just a moment for the process to kick off before reassigning stdout and stderr back to their
    # original streams.
    time.sleep(delay_s)

    # Reassign stdout and stderr back to their original streams.
    sys.stdout = old_stdout
    sys.stderr = old_stderr

    return proc
