import bz2
import gzip
import io
import lzma
import os
import tempfile
from pathlib import Path

import snappy

from dcw.etl.load import ListLoader, TextFileLoader, ParquetLoader
from dcw.storage.filesystem import Directory

import pandas as pd


def test_list_loader():
    """Test that the list loader works as expected."""
    loader = ListLoader()
    input = [1, 2, 3]
    for item in input:
        loader.load(item)
    assert loader.records == input, f"Loaded data does not match: {loader.records} != {input}"


def test_list_loader_destination_list():
    """Test that the list loader works as expected when provided a destination list."""
    my_list = []
    loader = ListLoader(my_list)
    input = [1, 2, 3]
    for item in input:
        loader.load(item)
    assert my_list == input, f"Loaded data does not match: {loader.records} != {input}"
    assert loader.records == input, f"Loaded data does not match: {loader.records} != {input}"


def test_text_file_loader():
    """Test that TextFileLoader loads files to the specified location."""
    def name_file(x):
        return f"{x}.txt"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=name_file)
        input = ["a", "b", "c"]
        for item in input:
            loader.load(item)

        assert sorted(os.listdir(tmpdir)) == [name_file(x) for x in input]


def test_text_file_loader_accepts_storage_object():
    """Test that TextFileLoader loads files to the specified location."""
    def name_file(x):
        return f"{x}.txt"

    with tempfile.TemporaryDirectory() as tmpdir:
        storage = Directory(tmpdir)
        loader = TextFileLoader(storage, filename_func=name_file)
        input = ["a", "b", "c"]
        for item in input:
            loader.load(item)

        assert sorted(os.listdir(tmpdir)) == sorted([name_file(x) for x in input])


def test_text_file_load_gzip_and_readback():
    data = "this is some data to compress and save"
    filename = "test.txt.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=lambda x: filename, compression="gzip")
        loader.load(data)

        assert os.path.exists(os.path.join(tmpdir, filename)), f"expected {filename} to exist"

        with gzip.open(os.path.join(tmpdir, filename), mode="rt") as f:
            assert f.read() == data, "expected to read back the same data as was written"


def test_text_file_load_xz_and_readback():
    data = "this is some data to compress and save"
    filename = "test.txt.xz"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=lambda x: filename, compression="xz")
        loader.load(data)

        assert os.path.exists(os.path.join(tmpdir, filename)), f"expected {filename} to exist"

        with lzma.open(os.path.join(tmpdir, filename), mode="rt") as f:
            assert f.read() == data, "expected to read back the same data as was written"


def test_text_file_load_bz2_and_readback():
    data = "this is some data to compress and save"
    filename = "test.txt.bz2"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=lambda x: filename, compression="bz2")
        loader.load(data)

        assert os.path.exists(os.path.join(tmpdir, filename)), f"expected {filename} to exist"

        with bz2.open(os.path.join(tmpdir, filename), mode="rt") as f:
            assert f.read() == data, "expected to read back the same data as was written"


def test_text_file_load_snappy_and_readback():
    data = "this is some data to compress and save"
    filename = "test.txt.snappy"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=lambda x: filename, compression="snappy")
        loader.load(data)

        assert os.path.exists(os.path.join(tmpdir, filename)), f"expected {filename} to exist"

        # read snappy file
        with open(os.path.join(tmpdir, filename), "rb") as f:
            decompressed = snappy.uncompress(f.read(), decoding="utf-8")
            assert decompressed == data, "expected to read back the same data as was written"


def test_text_file_loader_applies_serializer():
    """Test that TextFileLoader properly names and serializes text files for custom records."""
    # This is a bit of a contrived example, but it demonstrates the point.
    # We have a custom class, Foo, that represent records of data that we want to write to text files.
    class Foo:
        def __init__(self, val):
            self.val = val

    def name_file(obj: Foo):
        return f"{obj.val}.txt"

    def serializer(obj: Foo):
        return f"val: {obj.val}"

    with tempfile.TemporaryDirectory() as tmpdir:
        loader = TextFileLoader(tmpdir, filename_func=name_file, serializer=serializer)

        # input represents the records we want to load
        input = [Foo("a"), Foo("b"), Foo("c")]

        for item in input:
            loader.load(item)

        # Check that only the expected files were created
        assert sorted(os.listdir(tmpdir)) == [name_file(x) for x in input], "Expected 3 files: a.txt, b.txt, c.txt"

        # Check that the contents of the files are as expected
        for item in input:
            filename = name_file(item)
            expected_content = serializer(item)
            with open(os.path.join(tmpdir, filename)) as f:
                content = f.read()
                assert content == expected_content, f"Expected file contents to be '{expected_content}' got '{content}'"


def test_parquet_load_to_file():
    """Test that ParquetLoader loads files to the specified location."""
    data = """symbol,date,price
    XYZ,2021-01-01,100
    XYZ,2021-01-02,101
    """

    data = pd.read_csv(io.StringIO(data), parse_dates=["date"])

    with tempfile.TemporaryDirectory() as tmpdir:
        filename = Path(tmpdir) / "data.parquet"
        loader = ParquetLoader(filename)
        loader.load(data)
        assert filename.exists(), f"Expected file {filename} to exist"
        read_from_disk = pd.read_parquet(filename)
        assert read_from_disk.equals(data), f"Expected data to match: {read_from_disk} != {data}"


def test_parquet_load_to_partitioned():
    """Test that ParquetLoader loads files to the specified location in partitions."""
    data = """symbol,date,price
    ABC,2021-01-01,200
    ABC,2021-01-02,201
    XYZ,2021-01-01,100
    XYZ,2021-01-02,101
    """

    data = pd.read_csv(io.StringIO(data), parse_dates=["date"])

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        loader = ParquetLoader(output_dir, partition_key=lambda x: x["symbol"])
        loader.load(data)

        # Ensure the proper files were written, one for each partition present
        assert sorted((x.name for x in output_dir.glob("*"))) == ["ABC.parquet", "XYZ.parquet"]

        readback = pd.read_parquet(output_dir)
        assert readback.equals(data), "Expected reading back loaded data to match original data"
