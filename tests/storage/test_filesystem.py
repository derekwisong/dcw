import os
from pathlib import Path
import tempfile

import pytest

import dcw.storage.filesystem as fs


def test_directory_list():
    with tempfile.TemporaryDirectory() as tmpdir:
        num_files = 10
        for i in range(num_files):
            filename = f"test{i}.txt"
            open(Path(tmpdir) / filename, mode="w").close()

        # create a Directory object from an existing directory and ensure it finds the files
        input_dir = fs.Directory(tmpdir)
        files = input_dir.list()
        assert len(files) == num_files, f"Expected {num_files} files but got {len(files)}"


def test_directory_write():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = fs.Directory(tmpdir)
        filename = "test.txt"
        content = "Hello World!"

        with output_dir.open(filename, mode="w") as f:
            f.write(content)

        test_file = output_dir.path / filename
        assert test_file.exists(), f"Expected file {test_file} to exist"
        with test_file.open() as f:
            assert f.read() == content, f"Expected file contents to be '{content}' but got '{f.read()}'"


def test_directory_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        filename = "test.txt"
        content = "Hello World!"

        with open(Path(tmpdir) / filename, mode="w") as f:
            f.write(content)

        input_dir = fs.Directory(tmpdir)
        with input_dir.open(filename, mode="r") as f:
            assert f.read() == content, f"Expected file contents to be '{content}' but got '{f.read()}'"


def test_write_and_readback():
    with tempfile.TemporaryDirectory() as tmpdir:
        directory = fs.Directory(tmpdir)
        filename = "test.txt"
        content = "Hello World!"
        with directory.open(filename, mode="w") as f:
            f.write(content)

        with directory.open(filename, mode="r") as f:
            assert f.read() == content, f"Expected file contents to be '{content}' but got '{f.read()}'"


def test_directory_exists_returns_true_for_existing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        directory = fs.Directory(tmpdir)
        filename = "test.txt"
        content = "Hello World!"
        with directory.open(filename, mode="w") as f:
            f.write(content)

        assert directory.exists(filename), f"Expected file {filename} to exist"


def test_directory_exists_returns_false_for_non_existing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        directory = fs.Directory(tmpdir)
        filename = "test.txt"
        assert not directory.exists(filename), f"Expected file {filename} to not exist"


def test_directory_open_write_with_create_parents_for_deep_relative_path():
    # relative path within tmpdir, check with and without leading slash
    filenames = ["/foo/bar/test.txt", "foo/bar/baz/test.txt"]

    for filename in filenames:
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = fs.Directory(tmpdir, create_parents=True)

            with directory.open(filename, mode="w") as f:
                f.write("hello world")

            assert os.path.exists(f"{tmpdir}/{filename}"), f"Expected file {filename} to exist at expected path"
            assert directory.exists(filename), f"Expected Directory {filename} to exist"


def test_directory_open_write_without_create_parents_raises():
    """Test that using create_parents=False raises a FileNotFoundError when opening a file for writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        directory = fs.Directory(tmpdir, create_parents=False)

        with pytest.raises(FileNotFoundError):
            with directory.open("/foo/bar/test.txt", mode="w") as f:
                f.write("Hello World!")
