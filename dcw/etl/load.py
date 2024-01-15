import abc
import bz2
import gzip
import json
import logging
import lzma
import snappy
import pandas as pd
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from dcw.storage.filesystem import Directory, FileStorage

logger = logging.getLogger(__name__)


class Loader(abc.ABC):
    """Protocol for a data loader that typically stores processed data somewhere."""
    @abc.abstractmethod
    def load(self, item: Any) -> None:
        raise NotImplementedError("load() must be implemented by subclasses")

    def flush(self):
        """Flush any buffered data."""
        pass

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.load(*args, **kwargs)


class ListLoader(Loader):
    """A loader that stores records in a list.

    This loader is useful for testing purposes and for very small datasets.

    Attributes:
        records: The list of records that have been loaded.
    """

    def __init__(self, records: list = None):
        self.records = records if records is not None else []

    def load(self, item: Any) -> None:
        self.records.append(item)


class PrintLoader(Loader):
    """A loader that prints records to stdout.

    This loader is useful for debugging purposes.
    """

    def load(self, item: Any) -> None:
        print(item)


class JsonLinesLoader(Loader):
    """A loader that appends reconds to JSON lines (.jsonl) files.
    """

    def __init__(self,
                 path: str | Path,
                 *,
                 partition_key: Optional[Callable[[Any], Any]] = None,
                 dumper: Optional[Callable[[Any], Any]] = None):
        self.path = Path(path)
        self.partition_key = partition_key
        self.dumper = dumper

    def load(self, item: Any) -> None:
        output_file = self.path if self.partition_key is None else self.path / f"{self.partition_key(item)}.jsonl"

        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True, exist_ok=True)

        with output_file.open("a") as f:
            json.dump(self.dumper(item) if self.dumper else item, f)
            f.write("\n")


class ParquetLoader(Loader):
    """A loader that appends records to Parquet files.
    """

    def __init__(self,
                 path: str | Path,
                 *,
                 partition_key: Optional[Callable[[Any], Any]] = None):
        self.path = Path(path)
        self.partition_key = partition_key

    def _write_to_parquet(self, df: pd.DataFrame, output_file: Path) -> None:
        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True, exist_ok=True)

        if output_file.exists():
            existing_df = pd.read_parquet(output_file)
            df = pd.concat([existing_df, df], ignore_index=True)

        df.to_parquet(output_file, index=False, compression="snappy")

    def load(self, item: pd.DataFrame) -> None:
        if self.partition_key:
            for par, df in item.groupby(self.partition_key(item)):
                output_file = self.path / f"{str(par).strip()}.parquet"
                self._write_to_parquet(df, output_file)
        else:
            output_file = self.path
            self._write_to_parquet(item, output_file)


class TextFileLoader(Loader):
    """Load to text files.

    Attributes:
        output_dir: The directory to write files to.
        serializer: A function that converts a record to a string.
        filename_func: A function that returns the filename to write a record to.
        existing_behavior: What to do if the file already exists. One of "error", "skip", or "overwrite".
        compression: The compression to use. One of "gzip", "bz2", "xz", or None.
    """

    def __init__(self, output_dir: FileStorage | str | Path, *,
                 filename_func: Callable[[Any], str],
                 serializer: Callable[[Any], str] = None,
                 existing_behavior: Literal["error", "skip", "overwrite"] = "error",
                 compression: Literal["gzip", "bz2", "xz", None] = None):
        """Create the TextFileLoader loader.

        Attributes:
            output_dir: The directory to write files to. If a path or string is provided, it is assumed to be a path to
                a local directory.
            serializer: A function that converts a record to a string.
            filename_func: A function that returns the filename to write a record to.
            existing_behavior: What to do if the file already exists. One of "error", "skip", or "overwrite".
            compression: The compression to use. One of "gzip", "bz2", "xz", or None.
        """
        self.output_dir = Directory(Path(output_dir)) if isinstance(output_dir, (str, Path)) else output_dir
        self.serializer = serializer
        self.filename_func = filename_func
        self.existing_behavior = existing_behavior
        self.compression = compression

    def _write_uncompressed_to_file(self, path: str, data: str) -> None:
        with self.output_dir.open(path, "w") as f:
            f.write(data)

    def _write_gzip_to_file(self, path: Path, data: str) -> None:
        with self.output_dir.open(path, "wb") as f:
            f.write(gzip.compress(data.encode(errors="ignore")))

    def _write_bz2_to_file(self, path: Path, data: str) -> None:
        with self.output_dir.open(path, "wb") as f:
            f.write(bz2.compress(data.encode(errors="ignore")))

    def _write_xz_to_file(self, path: Path, data: str) -> None:
        with self.output_dir.open(path, "wb") as f:
            f.write(lzma.compress(data.encode(errors="ignore")))

    def _write_snappy_to_file(self, path: Path, data: str) -> None:
        with self.output_dir.open(path, "wb") as f:
            f.write(snappy.compress(data.encode(errors="ignore")))

    def _write_to_file(self, path: str, data: str) -> None:
        if self.compression is None or self.compression == "":
            self._write_uncompressed_to_file(path, data)
        elif self.compression == "gzip":
            self._write_gzip_to_file(path, data)
        elif self.compression == "bz2":
            self._write_bz2_to_file(path, data)
        elif self.compression == "xz":
            self._write_xz_to_file(path, data)
        elif self.compression == "snappy":
            self._write_snappy_to_file(path, data)
        else:
            raise ValueError(f"Unknown compression {self.compression}")

    def load(self, record: Any) -> None:
        """Save a record to a file."""
        filename = self.filename_func(record)

        if self.output_dir.exists(filename):
            if self.existing_behavior == "error":
                logger.debug(f"File {filename} already exists")
                raise FileExistsError(f"File {filename} already exists")
            elif self.existing_behavior == "skip":
                logger.debug(f"Skipping existing file {filename}")
                return
            elif self.existing_behavior == "overwrite":
                logger.debug(f"Overwriting existing file {filename}")
                pass
            else:
                raise ValueError(f"Unknown existing_behavior {self.existing_behavior}")

        logger.debug(f"Writing record to {filename}")
        try:
            self._write_to_file(filename, self.serializer(record) if self.serializer else record)
        except Exception as e:
            logger.error(f"Error writing record to {filename}: {e}")
            raise
