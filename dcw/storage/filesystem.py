from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol
from contextlib import contextmanager


@dataclass
class File:
    name: str
    size: int


class FileStorage(Protocol):
    @contextmanager
    def open(self, path: str | Path, mode="r") -> IO:
        raise NotImplementedError("open must be implemented")

    def list(self, path: str | Path) -> list[File]:
        raise NotImplementedError("list must be implemented")

    def exists(self, path: str | Path) -> bool:
        raise NotImplementedError("exists must be implemented")


class Directory(FileStorage):
    """A FileStorage implementation for local directories.

    Attributes:
        path (Path): The path to the directory.
        create_parents (bool): Whether to create parent directories if they don't exist when opening files for
            writing. Defaults to True.
    """

    def __init__(self, path: str | Path, create_parents: bool = True) -> None:
        """Create a new Directory object.

        Args:
            path (str | Path): The path to the directory.
            create_parents (bool): Whether to create parent directories if they don't exist when opening files for
                writing. Defaults to True.

        Raises:
            ValueError: If the path exists but is not a directory.
        """
        self.path: Path = Path(path)
        self.create_parents = create_parents

        if self.path.exists() and not self.path.is_dir():
            raise ValueError(f"Path is not a directory: {path}")

    def _make_relative(self, relative_path: str | Path) -> Path:
        relative_path = Path(relative_path)
        return self.path / relative_path.relative_to(relative_path.anchor)

    @contextmanager
    def open(self, relative_path: str | Path, mode: str = "r") -> IO:
        """Open a file in the directory.

        Args:
            relative_path (str | Path): The path to the file, relative to the directory.
            mode (str): The mode to open the file in. Defaults to "r". When opening a file for writing, parent
                directories will be created if they don't exist and the `create_parents` attribute is True.

        Yields:
            IO: A file-like object.
        """
        path = self._make_relative(relative_path)

        if mode.startswith("w") and self.create_parents:
            path.parent.mkdir(parents=True, exist_ok=True)

        with path.open(mode=mode) as fileobj:
            yield fileobj

    def list(self) -> list[File]:
        """List the contents of the directory.

        Returns:
            list[File]: A list of File objects.
        """
        files = []
        for file in self.path.iterdir():
            stat = file.stat()
            files.append(File(name=str(file), size=stat.st_size))
        return files

    def exists(self, relative_path: str | Path) -> bool:
        """Check if a file exists in the directory.

        Args:
            relative_path (str | Path): The path to the file, relative to the directory.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        path = self._make_relative(relative_path)
        return path.exists()
