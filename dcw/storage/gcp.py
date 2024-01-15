from contextlib import contextmanager
from typing import IO, List
from google.cloud import storage

from .filesystem import File, FileStorage


class Bucket(FileStorage):
    def __init__(self, bucket: str, root: str | None = None):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.root = root

    def _pathtrim(self, path: str) -> str:
        # remove leading and trailing slashes
        if path and path.startswith("/"):
            path = path[1:]

        if path and path.endswith("/"):
            path = path[:-1]

        return path

    def _makepath(self, path: str) -> str:
        if self.root:
            path = f"{self.root}/{self._pathtrim(path)}"

        path = self._pathtrim(path)
        path = path.replace("\\", "/")
        return path

    @contextmanager
    def open(self, path, mode="r") -> IO:
        """Open a file in the bucket."""
        path = self._makepath(path)
        blob = self.bucket.blob(path)
        with blob.open(mode=mode) as fileobj:
            yield fileobj

    def _list_directories(self, path=None) -> List[File]:
        """List the directories in the bucket."""
        blobs = []
        remote_blobs = self.bucket.list_blobs(prefix=path, delimiter="/", max_results=1)
        try:
            next(remote_blobs, ...)
        except StopIteration:
            # ignore this, the bucket might have no blobs, but we have to force them to load
            # before prefixes become available
            pass

        for prefix in remote_blobs.prefixes:
            blobs.append(
                File(
                    name=prefix,
                    size=0
                ))
        return blobs

    def list(self, path=None) -> List[File]:
        """List the contents of the bucket."""
        blobs = []
        path_cleaned = f"{self._pathtrim(path)}/"
        directories = self._list_directories(path_cleaned)
        blobs.extend(directories)

        blob: storage.Blob
        for blob in self.bucket.list_blobs(prefix=path_cleaned, delimiter="/"):
            blobs.append(
                File(
                    name=blob.name,
                    size=blob.size
                ))

        return blobs

    def exists(self, path) -> bool:
        """Check if a path exists in the bucket."""
        path = self._makepath(path)
        blob = self.bucket.blob(path)
        return blob.exists()

    @property
    def name(self):
        """The Bucket's name."""
        return self.bucket.name
