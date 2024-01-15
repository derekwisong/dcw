import os
import pytest
from pathlib import Path

from dcw.storage.gcp import Bucket
from dcw.storage.factory import file_storage_factory
from dcw.storage.filesystem import Directory

mock_bucket_name = "mock-bucket-123abc0fe"  # name to use for mock buckets
IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_storage_factory_return_bucket():
    """Test that storage_factory returns a Bucket object when given a gs:// path."""
    storage = file_storage_factory(f"gs://{mock_bucket_name}")
    assert isinstance(storage, Bucket)
    assert storage.name == mock_bucket_name


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_storage_factory_return_bucket_when_given_full_path():
    """Test that storage_factory returns a Bucket object when given a full gs://<bucket>/<path...>."""
    storage = file_storage_factory(f"gs://{mock_bucket_name}/path/to/some/place")
    assert isinstance(storage, Bucket)
    assert storage.name == mock_bucket_name


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skipping test because running on github actions")
def test_storage_factory_returns_directory_when_given_path():
    """Test that storage_factory returns a Directory object when given a path."""
    path = "/path/to/some/place"
    storage = file_storage_factory(path)
    assert isinstance(storage, Directory)
    assert storage.path == Path(path)


def test_storage_factory_returns_directory_when_given_pathlib_path():
    """Test that storage_factory returns a Directory object when given a pathlib.Path."""
    path = Path("/path/to/some/place")
    storage = file_storage_factory(path)
    assert isinstance(storage, Directory)
    assert storage.path == path


def test_storage_factory_raises_for_unknown_scheme_in_uri():
    """Test that storage_factory raises a ValueError when given a URI with an unknown scheme."""
    with pytest.raises(ValueError):
        file_storage_factory("doesnotexist://some/non-existant/path")
