"""Factories for creating storage objects."""

from pathlib import Path
from urllib.parse import urlparse

from .gcp import Bucket
from .filesystem import Directory, FileStorage


def file_storage_factory(path_or_uri: str | Path) -> FileStorage:
    """Get a storage object based on the provided path.

    Args:
        path_or_uri (str | Path): The path to the storage location. This may be a URI or a filesystem path. If a `Path`
            instance is provided, a `Directory` object will be returned.

    Returns:
        FileStorage: An object that implements the `FileStorage` protocol.

    """
    if isinstance(path_or_uri, Path):
        return Directory(path_or_uri)

    parsed = urlparse(path_or_uri)
    if not parsed.scheme:
        return Directory(parsed.path)
    elif parsed.scheme == "gs":
        return Bucket(parsed.netloc, root=parsed.path)
    else:
        raise ValueError(f"Unable to create storage object for path: {path_or_uri}")
