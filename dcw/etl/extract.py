import json
import logging
from pathlib import Path
from typing import Any, Callable, Iterator, Protocol


logger = logging.getLogger(__name__)


class Extractor(Protocol):
    """Protocol for a data extractor that can produce records."""

    def iter_records(self) -> Iterator[Any]:
        """Iterate over records."""
        raise NotImplementedError("Extractor must implement iter_records()")


class RecordExtractor(Extractor):
    """A simple extractor that iterates over an object and yields each item as a record."""

    def __init__(self, data):
        self.data = data

    def iter_records(self):
        for item in self.data:
            yield item


class FileWalkExtractor(Extractor):
    """An extractor that walks a directory and yields each `pathlib.Path` as a record.

    Attributes:
        path (pathlib.Path): The path to walk.
        recursive (bool): Whether to walk recursively.
        glob (str): The glob pattern to match.
    """

    def __init__(self, path: str | Path, *, recursive: bool = False, glob: str = "*", files_only: bool = True):
        """Initialize the extractor.

        Arguments:
            path (str | pathlib.Path): The path to walk.
            recursive (bool): Whether to walk recursively.
            glob (str): The glob pattern to match.
            files_only (bool): If True, only extract/yield files (avoid directories).
        """
        self.path = Path(path)
        self.recursive = recursive
        self.glob = glob
        self.files_only = files_only

    def iter_records(self) -> Iterator[Path]:
        """Iterate over the files in the path."""
        for path in self.path.rglob(self.glob) if self.recursive else self.path.glob(self.glob):
            if self.files_only and path.is_dir():
                continue
            yield path


class JsonFileExtractor(Extractor):
    """An extractor that walks a directory and yields each JSON file as a (path, data) tuple."""

    def __init__(self, file_or_dir_path: str | Path, *, glob="*.json",
                 on_error: Callable[[Path, Exception], None] | None = None):
        """Create the extractor.

        Arguments:
            file_or_dir_path (str | pathlib.Path): The path to walk.
            glob (str): The glob pattern to match.
            on_error (Callable[[Path, Exception], None] | None): A callback to invoke when an error occurs."""
        self.path = Path(file_or_dir_path)
        self.glob = glob
        self.on_error = on_error

    def iter_records(self) -> Iterator[tuple[Path, Any]]:
        """Iterate over the files in the path and yield each file as a (path, data) tuple.

        If the data cannot be parsed as JSON, the `on_error` callback is invoked (if provided) or an exception is
        raised, which may be caught by the caller.

        Yields:
            tuple[pathlib.Path, Any]: A tuple containing the path to the file and the JSON data parsed from the file.
        """
        extractor = FileWalkExtractor(self.path, files_only=True, glob=self.glob, recursive=True)
        for path in extractor.iter_records():
            try:
                with path.open() as f:
                    yield path, json.load(f)
            except Exception as e:
                if self.on_error is not None:
                    self.on_error(path, e)
                else:
                    raise
