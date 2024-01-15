import datetime
import json

import os

import pytest

from dcw.storage.gcp import Bucket

from dcw.etl.load import TextFileLoader


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"
TEST_BUCKET = os.getenv("DCW_TEST_BUCKET")
TEST_FILE_ROOT = "dcw-test"
TEST_FILE_NAME = "dcw-test-file"
TEST_FILE = f"{TEST_FILE_ROOT}/{TEST_FILE_NAME}"  # test file that numerous tests my read and write to


@pytest.fixture(scope="session")
def bucket() -> Bucket:
    bkt = Bucket(TEST_BUCKET)
    return bkt


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_test_bucket_is_set():
    assert TEST_BUCKET is not None, "DCW_TEST_BUCKET environment variable must be set"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_bucket_builds_path_correctly():
    """Test that the Bucket builds paths correctly."""
    bucket = Bucket("foo-bucket")
    assert bucket._makepath("foo") == "foo"
    assert bucket._makepath("/foo") == "foo"
    assert bucket._makepath("foo/bar") == "foo/bar"
    assert bucket._makepath("/foo/bar") == "foo/bar"
    assert bucket._makepath("foo\\bar") == "foo/bar"
    assert bucket._makepath("/foo\\bar") == "foo/bar"
    assert bucket._makepath("foo\\bar\\baz") == "foo/bar/baz"
    assert bucket._makepath("/foo\\bar\\baz") == "foo/bar/baz"
    assert bucket._makepath("foo/bar/baz") == "foo/bar/baz"
    assert bucket._makepath("/foo/bar/baz") == "foo/bar/baz"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_bucket_with_root_builds_path_correctly():
    """Test that the Bucket builds paths correctly when a root is specified."""
    bucket = Bucket("foo-bucket", root="bar")
    assert bucket._makepath("foo") == "bar/foo"
    assert bucket._makepath("/foo") == "bar/foo"
    assert bucket._makepath("foo/bar") == "bar/foo/bar"
    assert bucket._makepath("/foo/bar") == "bar/foo/bar"
    assert bucket._makepath("foo\\bar") == "bar/foo/bar"
    assert bucket._makepath("/foo\\bar") == "bar/foo/bar"
    assert bucket._makepath("foo\\bar\\baz") == "bar/foo/bar/baz"
    assert bucket._makepath("/foo\\bar\\baz") == "bar/foo/bar/baz"
    assert bucket._makepath("foo/bar/baz") == "bar/foo/bar/baz"
    assert bucket._makepath("/foo/bar/baz") == "bar/foo/bar/baz"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_bucket_with_root_and_forward_slashes_removes_leading_slash():
    """Test that the Bucket builds paths correctly when a root is specified and the root has a leading slash."""
    bucket = Bucket("foo-bucket", root="/bar")
    assert bucket._makepath("foo") == "bar/foo"
    assert bucket._makepath("/foo") == "bar/foo"
    assert bucket._makepath("foo/bar") == "bar/foo/bar"
    assert bucket._makepath("/foo/bar") == "bar/foo/bar"
    assert bucket._makepath("foo\\bar") == "bar/foo/bar"
    assert bucket._makepath("/foo\\bar") == "bar/foo/bar"
    assert bucket._makepath("foo\\bar\\baz") == "bar/foo/bar/baz"
    assert bucket._makepath("/foo\\bar\\baz") == "bar/foo/bar/baz"
    assert bucket._makepath("foo/bar/baz") == "bar/foo/bar/baz"
    assert bucket._makepath("/foo/bar/baz") == "bar/foo/bar/baz"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_write_and_readback(bucket: Bucket):
    data = json.dumps(
        {
            "timestamp": datetime.datetime.utcnow().isoformat(),
        },
        indent=2)

    # write a status file
    with bucket.open(TEST_FILE, mode="w") as status_file:
        status_file.write(data)

    # read the status file back and assert that the content is the same as what was written
    # NOTE: Race condition alert, a subsequent write before the read could happen and change the contents of the file
    with bucket.open(TEST_FILE, mode="r") as status_file:
        assert status_file.read() == data, f"Data read back from {TEST_FILE} is not the same as what was written"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_write_and_readback_using_root():
    """Test that writing to a file using a Bucket that includes a root directory puts files in the expected place"""
    data = json.dumps(
        {
            "timestamp": datetime.datetime.utcnow().isoformat(),
        },
        indent=2)

    bucket = Bucket(TEST_BUCKET, root=TEST_FILE_ROOT)
    # write a status file
    with bucket.open(TEST_FILE_NAME, mode="w") as status_file:
        status_file.write(data)

    with Bucket(TEST_BUCKET).open(TEST_FILE, mode="r") as status_file:
        assert status_file.read() == data, f"Data read back from {TEST_FILE} is not the same as what was written"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_list_directory(bucket: Bucket):
    """Test listing a directory that is expected to have 1 file in it"""
    # test without trailing slash
    contents = bucket.list(f"{TEST_FILE_ROOT}")
    assert len(contents) == 1
    assert contents[0].name == TEST_FILE

    # test with slash
    contents = bucket.list(f"{TEST_FILE_ROOT}/")
    assert len(contents) == 1
    assert contents[0].name == TEST_FILE


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_exists_returns_true_for_existing_file(bucket: Bucket):
    with bucket.open(TEST_FILE, mode="w") as status_file:
        status_file.write("")

    assert bucket.exists(TEST_FILE)


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_exists_returns_false_for_nonexistent_file(bucket: Bucket):
    assert not bucket.exists("/this_path_should_not_exist/adsf987asd9gjklfgjh3942r43kjlfgd.txt")


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_text_file_loader(bucket: Bucket):
    """Test that TextFileLoader loads files to the specified bucket."""
    loader = TextFileLoader(bucket, filename_func=lambda x: TEST_FILE, existing_behavior="overwrite")
    data = "Hello World, from test_text_file_loader"
    loader.load(data)

    with bucket.open(TEST_FILE, mode="r") as f:
        assert f.read() == data, f"Expected file contents to be '{data}' but got '{f.read()}'"
