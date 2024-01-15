import gzip
from typing import Any, Protocol


class Transformation(Protocol):
    """Protocol for a callable data transformation class."""

    def transform(self, item: Any) -> Any:
        pass

    def __call__(self, item: Any) -> Any:
        return self.transform(item)


class SquareTransformation(Transformation):
    """A simple transformation that squares the input."""

    def transform(self, record):
        return record ** 2


class GzipExtractTransformation(Transformation):
    """A transformation that extracts gzipped data."""

    def __init__(self, encoding: str | None = None):
        """Create a new GzipExtractTransformation.

        Args:
            encoding: The encoding to use when decoding the data. If None, returns bytes.
        """
        self.encoding = encoding

    def transform(self, record) -> str | bytes:
        """Extract gzipped data from the source."""
        with gzip.open(record, mode="rb") as f:
            data = f.read()
            return data.decode(self.encoding) if self.encoding else data
