import os
import sys

from typing import IO


def supports_color(stream: IO = sys.stdout) -> bool:
    """Determines if the stream supports color.

    Args:
        stream (IO): The stream to check. Default: `sys.stdout`.

    Returns:
        bool: True if the running system's terminal supports color, and False otherwise.
    """
    plat = sys.platform
    supported_platform = plat != 'Pocket PC' and (plat != 'win32' or 'ANSICON' in os.environ)
    is_a_tty = hasattr(stream, 'isatty') and stream.isatty()
    return supported_platform and is_a_tty
