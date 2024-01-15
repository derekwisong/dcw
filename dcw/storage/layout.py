import datetime
import gzip
import logging
from io import BytesIO
import json
from pathlib import PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from dcw.timing import Timer

from .backend import StorageBackend


logger = logging.getLogger(__name__)


class TimestampedFiles:
    """Operate on files organized by timestamped paths.
    """

    def __init__(self, backend: StorageBackend, *, root: Optional[str] = None, timestamp_path: bool = True,
                 compress: bool = True, metadata: Optional[Dict[str, Any]] = None,
                 file_timestamp: Optional[datetime.datetime] = None) -> None:
        """
        Arguments:
            backend: The type of storage that files will be saved into.
            root: Optional. The root path that all files will be relative to.
            timestamp_path: Default: True. If True, time format codes in paths will be replaced (see datetime.strftime).
            compress: Default: True. If True, gzip compress data being written.
            file_timestamp: Optional. A timestamp to base file path substitution on.
            metadata: Optional. If a value is provided, a metadata JSON file will be saved by adding an additional
                ".metadata.json" suffix to the path being written. The value provided to `metadata` will be stored in
                the "data" key of the metadata JSON.
        """
        self.engine = backend
        self.root = root if isinstance(root, PurePath) else PurePath(root) if root else None
        self.timestamp_path = timestamp_path
        self.compress = compress
        self.metadata = metadata
        self.file_timestamp = file_timestamp

    def _finalize_path(self, path: Union[str, PurePath], timestamp: datetime.datetime) -> str:
        if self.root:
            path = self.root / path

        path = f"{path}"

        if self.timestamp_path:
            path = path.format(timestamp="%Y%m%d_%H%M%S")
            path = timestamp.strftime(path)

        metadata_path = f"{path}.metadata.json" if self.metadata else None

        if self.compress:
            path = f"{path}.gz"
            if self.metadata:
                metadata_path = f"{metadata_path}.gz"

        return path, metadata_path

    def _preprocess_data(self, data: Any) -> bytes:
        if isinstance(data, str):
            data = data.encode("utf-8")

        stream = BytesIO()

        if self.compress:
            gzip.GzipFile(fileobj=stream, mode="wb").write(data)
        else:
            stream.write(data)

        return stream.getvalue()

    def _write(self, path: Union[str, PurePath], data: Any):
        data = self._preprocess_data(data)

        logger.debug(f"Writing to {path}")
        with self.engine.open(path, mode="wb") as fileobj:
            fileobj.write(data)

    def write(self, path: Union[str, PurePath], data: Any = None, *, func: Optional[Callable] = None,
              args: Optional[Union[Tuple, List]] = None, kwargs: Optional[Dict[str, Any]] = None) -> None:
        with Timer() as collect_timer:
            if func:
                data = func(*args or tuple(), **kwargs or dict())

        file_timestamp = self.file_timestamp or collect_timer.end
        path, metadata_path = self._finalize_path(path, file_timestamp)

        with Timer() as write_timer:
            self._write(path, data)

        if not metadata_path:
            return

        metadata = {
            "collect_time": {
                "start": collect_timer.start.isoformat(),
                "end": collect_timer.end.isoformat()
            },
            "write_time": {
                "start": write_timer.start.isoformat(),
                "end": write_timer.end.isoformat()
            },
            "provided_file_timestamp": True if self.file_timestamp else False,
            "file_timestamp": file_timestamp.isoformat(),
            "data": None if self.metadata is True else self.metadata
        }

        self._write(metadata_path, json.dumps(metadata))
