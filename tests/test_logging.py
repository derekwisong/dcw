import io
import logging

import dcw.logging


def test_console_logging_default_colors():
    """Test that add_console_logging properly colorizes the output with the expected default colors."""
    logger = logging.getLogger("test_logging")
    logger.setLevel(logging.DEBUG)

    stream = io.StringIO()  # capture the log output to a string so it can be inspected
    dcw.logging.add_console_logging(logger,
                                    format="%(name)s: %(message)s",
                                    stream=stream,
                                    colorful=True,
                                    force_colorful=True)
    logger.debug("debug")
    logger.info("info")
    logger.warning("warning")
    logger.error("error")
    logger.critical("critical")

    # check that 5 lines of logging were output
    lines = stream.getvalue().strip().splitlines()
    assert 5 == len(lines)

    # check that all the log lines are properly colorized
    RESET = dcw.logging.AnsiColorCode("reset")
    assert lines[0] == f"{dcw.logging.AnsiColorCode('grey')}test_logging: debug{RESET}"
    assert lines[1] == f"{dcw.logging.AnsiColorCode('white')}test_logging: info{RESET}"
    assert lines[2] == f"{dcw.logging.AnsiColorCode('yellow')}test_logging: warning{RESET}"
    assert lines[3] == f"{dcw.logging.AnsiColorCode('red')}test_logging: error{RESET}"
    assert lines[4] == f"{dcw.logging.AnsiColorCode('bold_red')}test_logging: critical{RESET}"
