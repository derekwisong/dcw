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


def test_Batcher():
    dataset = [1, 2, 3, 4, 5, 6, 7]
    extractor = extract.RecordExtractor(dataset)
    batch_extractor = extract.Batcher(extractor, batch_size=2)
    results = list(batch_extractor.iter_records())
    assert results == [[1, 2], [3, 4], [5, 6], [7]]


def test_Batcher_large_batch_size():
    """Test that a batch_size larger than the dataset returns the entire dataset as a single batch."""
    dataset = [1, 2, 3, 4, 5, 6, 7]
    extractor = extract.RecordExtractor(dataset)
    batch_extractor = extract.Batcher(extractor, batch_size=200)
    results = list(batch_extractor.iter_records())
    assert results == [[1, 2, 3, 4, 5, 6, 7]]


def test_Batcher_zero_batch_size():
    """Test that a batch_size of 0 returns the entire dataset as a single batch."""
    dataset = [1, 2, 3, 4, 5, 6, 7]
    extractor = extract.RecordExtractor(dataset)
    batch_extractor = extract.Batcher(extractor, batch_size=0)
    results = list(batch_extractor.iter_records())
    assert results == [[1, 2, 3, 4, 5, 6, 7]]


def test_Unpacker():
    dataset = [[1, 2, 3], [4, 5, 6]]
    extractor = extract.RecordExtractor(dataset)
    unpacker = extract.Unpacker(extractor)
    results = list(unpacker.iter_records())
    assert results == [1, 2, 3, 4, 5, 6]