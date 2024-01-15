"""Tools for setting up logging.

Examples:
    Set up basic colorful logging to the console:
    >>> import logging
    >>> from dcw.logging import add_console_logging
    >>> # create a logger and set it to DEBUG level
    >>> logger = logging.getLogger(__name__)
    >>> logger.setLevel(logging.DEBUG)
    >>> # add a handler to output to the console and give each level a different color
    >>> add_console_logging(logger, colorful=True)
    >>> # log some messages
    >>> logger.debug("Hello, world!")
    >>> logger.info("Hello, world!")
    >>> logger.warning("Hello, world!")
    >>> logger.error("Hello, world!")
    >>> logger.critical("Hello, world!")

Attributes:
    DEFAULT_FORMAT: The default format string to use for log lines. This should be the format understood by the python
        `logging` module. Default: `%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)`.
    DEFAULT_DATE_FORMAT: The default date format string to use for log lines. The default value is ISO 8601.
"""
import logging
import sys

from .system import supports_color


DEFAULT_FORMAT: str = "%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)"
DEFAULT_DATE_FORMAT: str = "%Y-%m-%dT%H:%M:%S%z"


class AnsiColorCode:
    """An ANSI color control sequence.

    Examples:
        >>> AnsiColorCode("red").code()
        '\\x1b[31;20m'
        >>> AnsiColorCode((255, 0, 0)).code()
        '\\x1b[38;2;255;0;0m'

    Attributes:
        foreground: A dictionary of ANSI foreground color codes. The available colors are: black, red, green, yellow,
            blue, magenta, cyan, white, grey, bold_red, and reset (the color reset code).
    """
    foreground = {
        "black": "\x1b[30;20m",
        "red": "\x1b[31;20m",
        "green": "\x1b[32;20m",
        "yellow": "\x1b[33;20m",
        "blue": "\x1b[34;20m",
        "magenta": "\x1b[35;20m",
        "cyan": "\x1b[36;20m",
        "white": "\x1b[37;20m",
        "grey": "\x1b[38;20m",
        "bold_red": "\x1b[31;1m",
        "reset": "\x1b[0m"
    }

    def __init__(self, color: str | tuple[int, int, int]) -> None:
        """Create an ANSI foreground color.

        Args:
            color: The color to use. One of the keys in `foreground` or a tuple of (red, green, blue) values.
        """
        if isinstance(color, str):
            self.color = self.foreground[color]
        else:
            r, g, b = color
            if not 0 <= r <= 255:
                raise ValueError(f"Invalid red value {r}")
            if not 0 <= g <= 255:
                raise ValueError(f"Invalid green value {g}")
            if not 0 <= b <= 255:
                raise ValueError(f"Invalid blue value {b}")
            self.color = f"\x1b[38;2;{r};{g};{b}m"

    def code(self) -> str:
        """Get the ANSI escape code.

        Returns:
            str: The ANSI escape code.
        """
        return self.color

    def __str__(self) -> str:
        return self.code()


class ColorfulFormatter(logging.Formatter):
    """A logging formatter that uses ANSI escape codes to colorize the output.
    """

    def __init__(self,
                 fmt: str = None,
                 datefmt: str = None,
                 *,
                 debug: AnsiColorCode = AnsiColorCode("grey"),
                 info: AnsiColorCode = AnsiColorCode("white"),
                 warning: AnsiColorCode = AnsiColorCode("yellow"),
                 error: AnsiColorCode = AnsiColorCode("red"),
                 critical: AnsiColorCode = AnsiColorCode("bold_red")
                 ) -> None:
        """Create a ColorfulFormatter.

        Args:
            fmt: Optional. The format string to use. Default: `DEFAULT_FORMAT`.
            datefmt: Optional. The date format to use. Default: ISO 8601.
            debug: Optional. The ANSI escape code to use for DEBUG level messages. Default: `RESET`.
            info: Optional. The ANSI escape code to use for INFO level messages. Default: `RESET`.
            warning: Optional. The ANSI escape code to use for WARNING level messages. Default: `YELLOW`.
            error: Optional. The ANSI escape code to use for ERROR level messages. Default: `RED`.
            critical: Optional. The ANSI escape code to use for CRITICAL level messages. Default: `BOLD_RED`.
        """
        fmt = fmt or DEFAULT_FORMAT
        self.formatters = {
            level: logging.Formatter(f"{color}{fmt}{AnsiColorCode('reset')}", datefmt=datefmt)
            for level, color in zip(
                (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL),
                (debug, info, warning, error, critical))}

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record.

        Args:
            record: The log record to format.

        Returns:
            str: The formatted log record.
        """
        formatter = self.formatters.get(record.levelno)
        return formatter.format(record)


def add_console_logging(logger: logging.Logger, *,
                        colorful: bool = True,
                        datefmt: str = DEFAULT_DATE_FORMAT,
                        format: str = None,
                        level: int = None,
                        stream: None = None,
                        force_colorful: bool = False) -> None:
    """Add console output logging to the provided logger.

    By default, the handler will be set to INFO level and add `ColorfulFormatter` to the logger.

    Args:
        logger: The Logger to add the handler to.
        colorful: If True, and the terminal supports color, the formatter will colorize the output using ANSI escape
            codes. Default: True.
        datefmt: The date format to use for the formatter. Default: ISO 8601.
        format: Optional. The format string to use for the formatter. see `DEFAULT_FORMAT`.
        level: Optional. The level to set the handler to.
        stream: Optional. Stream to use for the handler. Default: `sys.stderr`.
        force_colorful: Optional. If True, and `colorful` is `True`, the formatter will colorize the output even if the
            terminal does not support color.
    """
    format = format or DEFAULT_FORMAT
    stream = stream or sys.stderr
    use_color = colorful and (force_colorful or supports_color(stream))
    formatter = ColorfulFormatter(format, datefmt=datefmt) if use_color else logging.Formatter(format, datefmt=datefmt)
    handler = logging.StreamHandler(stream)
    if level is not None:
        handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
