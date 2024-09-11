
import re

from pandas.core.array_algos.replace import compare_or_regex_search

# 7-bit C1 ANSI sequences
ansi_escape = re.compile(r'''
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     # or [ for CSI, followed by a control sequence
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
''', re.VERBOSE)

# Matches any lines that end in a carriage return.
cr_line_regex = re.compile(r'((?<=[\n\r]).*?\r)|(^.*?\r)')


def unformat(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return ansi_escape.sub("", text)


def delete_cr_lines(text: str) -> str:
    """Remove carriage returns and the lines preceding them from text."""
    return cr_line_regex.sub("", text)


def unformat_and_delete_cr_lines (text: str) -> str:
    """Remove ANSI escape codes and carriage returns and the lines preceding them from text."""
    return delete_cr_lines(unformat(text))

