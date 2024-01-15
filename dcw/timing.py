"""Utilities for measuring elapsed time.
"""
import datetime
from typing import Callable


class Timer:
    """A simple timer for measuring elapsed time.

    - Elapsed time is measured as the time since creation, or the last call to `reset()`.
    - When used as a context manager, the `Timer` will automatically stop when the block exits.

    Examples:
        Time a block using context manager:
        >>> import time
        >>> with Timer() as timer:
        ...     time.sleep(0.1)
        >>> round(timer.elapsed().total_seconds(), 1)
        0.1

        Time a block and print when it exits:
        >>> import time
        >>> with Timer(on_exit=lambda x: print(f"Block took: {round(x.elapsed().total_seconds(), 1)}s")) as timer:
        ...     time.sleep(0.1)
        Block took: 0.1s
    """

    def __init__(self, *, on_exit: Callable[["Timer"], None] | None = None) -> None:
        """Create a timer.

        Args:
            on_exit: A callback to call when the timer exits, if being used as a context manager. The callback will be
                passed the timer instance.
        """
        self.start = self.now()
        self.end = None
        self.on_exit = on_exit

    def now(self) -> datetime.datetime:
        """Get the current time in UTC"""
        return datetime.datetime.utcnow()

    def range(self) -> tuple[datetime.datetime, datetime.datetime]:
        """Get the start and end times in UTC.

        Returns:
            tuple: (start, end)"""
        return self.start, self.end or self.now()

    def elapsed(self) -> datetime.timedelta:
        """Get the elapsed time.

        Returns:
            datetime.timedelta: The elapsed time.
        """
        start, end = self.range()
        return end - start

    def stop(self) -> None:
        """Stop the timer."""
        self.end = self.now()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if self.on_exit:
            self.on_exit(self)
        return False
