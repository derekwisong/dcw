import json
import pathlib
from typing import Any, Iterator, Protocol


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


class Unpacker(Extractor):
    """An Extractor that unpacks a record from another Extractor and yields each item as a record.

    Example:

        Extraction pipeline that iterates sublists from a list of lists and yields each item as a record:

        >>> records = [[1, 2, 3], [4, 5, 6]]
        >>> extractor = Unpacker(RecordExtractor(records))
        >>> list(extractor.iter_records())
        [1, 2, 3, 4, 5, 6]
    """

    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    def iter_records(self):
        for record in self.extractor.iter_records():
            yield from record


class Batcher(Extractor):
    """An Extractor that batches records from another Extractor and yields each batch as a record.

    Example:

        Extraction pipeline that batches records from a list of numbers into lists of 2:

        >>> records = [1, 2, 3, 4, 5, 6]
        >>> extractor = Batcher(RecordExtractor(records), batch_size=2)
        >>> list(extractor.iter_records())
        [[1, 2], [3, 4], [5, 6]]
    """

    def __init__(self, extractor: Extractor, batch_size: int):
        self.extractor = extractor
        self.batch_size = batch_size

    def iter_records(self):
        batch = []
        for record in self.extractor.iter_records():
            batch.append(record)
            if len(batch) == self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


class JsonFileExtractor(Extractor):
    """An extractor that reads a JSON file and yields it as a single record."""

    def __init__(self, filepath):
        self.filepath = pathlib.Path(filepath)

    def iter_records(self):
        with open(self.filepath, "r") as f:
            yield json.load(f)


class FileWalkExtractor(Extractor):
    """An extractor that walks a directory and yields each file as a record.

    Attributes:
        path (pathlib.Path): The path to walk.
        recursive (bool): Whether to walk recursively.
        glob (str): The glob pattern to match.
    """

    def __init__(self, path: str | pathlib.Path, *, recursive: bool = False, glob: str = "*", files_only: bool = True):
        """Initialize the extractor.

        Arguments:
            path (str | pathlib.Path): The path to walk.
            recursive (bool): Whether to walk recursively.
            glob (str): The glob pattern to match.
            files_only (bool): If True, only extract/yield files (avoid directories).
        """
        self.path = pathlib.Path(path)
        self.recursive = recursive
        self.glob = glob
        self.files_only = files_only

    def iter_records(self) -> Iterator[pathlib.Path]:
        """Iterate over the files in the path."""
        for path in self.path.rglob(self.glob) if self.recursive else self.path.glob(self.glob):
            if self.files_only and path.is_dir():
                continue
            yield path
