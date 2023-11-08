"""
Utils Module

This module provides utility functions e.g. for formatted logs.

Dependencies:
- datetime: Provides functions to get time info.
- rich: Provides function for formatting the logs.
"""
import datetime
from rich import print as rich_print

def log(message: str, level: str ="INFO") -> None:
    """
    Logs a message with a timestamp and a specified level.
    The function uses `rich` library to format the output, highlighting
    the timestamp in white, level in red, and the message in green.

    :param message (str): The message to log.
    :param level (str): The logging level, defaults to "INFO".
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rich_print(f"[white]{timestamp}[/white] [red]{level}[/red] > [green]{message}[/green]")
