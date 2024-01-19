import json
import tempfile
from pathlib import Path
import dcw.etl.extract as extract


def test_RecordExtractor():
    dataset = [
        ("foo", 100),
        ("bar", 200),
        ("baz", 300),
    ]
    extractor = extract.RecordExtractor(dataset)
    for i, record in enumerate(extractor.iter_records()):
        assert record == dataset[i], f"Record[{i}] does not match: {record} != {dataset[i]}]"


def test_JsonFileExtractor():
    # various types of data to write to json files (key: filename, value: data)
    dataset = {
        "foo": 100,
        "bar": {"a": 1, "b": 2},
        "baz": ["a", 1],
        "bif": None,
        "box": True,
        "bzo": "string",
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # write some files to the tmpdir
        for name, data in dataset.items():
            with (tmpdir / f"{name}.json").open("w") as f:
                json.dump(data, f)

        # use the extractor to read the files
        extractor = extract.JsonFileExtractor(tmpdir)
        n = 0
        for path, data in extractor.iter_records():
            assert dataset[path.stem] == data
            n += 1

        assert n == len(dataset)


def test_JsonFileExtractor_callback_on_error():
    """Test that the callback is invoked when an error occurs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # write some files to the tmpdir
        with (tmpdir / "foo.json").open("w") as f:
            f.write("not json")

        # use the extractor to read the files
        errors = []

        def on_error(path, exc):
            errors.append((path, exc))

        extractor = extract.JsonFileExtractor(tmpdir, on_error=on_error)
        n = 0
        for path, data in extractor.iter_records():
            n += 1

        assert n == 0
        assert len(errors) == 1
        assert errors[0][0] == tmpdir / "foo.json"
        assert isinstance(errors[0][1], json.decoder.JSONDecodeError)
