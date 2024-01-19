import pytest

from dcw.etl.pipeline import ProcessingPipeline, PipelineFactory, run_pipeline
from dcw.etl.extract import Extractor, RecordExtractor
from dcw.etl.load import ListLoader, PrintLoader


def test_ProcessingPipeline():
    loader = ListLoader()

    pipeline = ProcessingPipeline()
    pipeline.add_transform(lambda x: x ** 2)
    pipeline.add_loader(loader)

    pipeline.push(1)
    pipeline.push(2)
    pipeline.push(3)

    assert loader.records == [1, 4, 9]


def test_dual_load_ProcessingPipeline():
    # create two loaders (which load items to their own internal lists)
    loader = ListLoader()
    loader_squares = ListLoader()

    def create_pipeline():
        source = ProcessingPipeline()

        # create a pipeline that loads to one list
        source.add_loader(loader)

        # create a pipeline that squares the source and loads it to another list
        source.add_transform(lambda x: x ** 2)
        source.add_loader(loader_squares)

        return source

    pipeline = create_pipeline()
    pipeline.push(1)
    pipeline.push(2)
    pipeline.push(3)

    assert loader.records == [1, 2, 3]
    assert loader_squares.records == [1, 4, 9]


def test_ProcessingPipeline_with_extractor():
    # initialize the data extractor class which will emit input data
    extractor = RecordExtractor([1, 2, 3])
    # set up a loader to store the pipeline output
    loader = ListLoader()

    # set up the pipeline
    pipeline = ProcessingPipeline()
    pipeline.add_transform(lambda x: x ** 2)
    pipeline.add_loader(loader)

    # run the pipeline on data from the extractor
    pipeline.extract(extractor)

    # check that the results were loaded as expected
    assert loader.records == [1, 4, 9]


def test_ProcessingPipeline_named():
    pipeline = ProcessingPipeline(name="test")
    assert pipeline.name == "test"
    assert pipeline.end_name == "test"


def test_ProcessingPipeline_transform_is_named():
    pipeline = ProcessingPipeline()
    pipeline.add_transform(lambda x: x ** 2, name="square")
    assert pipeline.end_name == "square"


def test_ProcessingPipeline_loader_is_named():
    pipeline = ProcessingPipeline()
    pipeline.add_loader(ListLoader(), name="test_loader")
    # TODO this assertion should be replaced with something that doesnt require knowledge of the internals
    assert list(pipeline.current.downstreams)[0].name == "test_loader"


def test_ProcessingPipeline_get_loaders():
    pipeline = ProcessingPipeline()
    loader1 = ListLoader()
    loader2 = ListLoader()
    pipeline.add_loader(loader1)
    pipeline.add_loader(loader2)
    assert list(pipeline.get_loaders()) == [loader1, loader2]


def test_ProcessingPipeline_batcher_is_named():
    pipeline = ProcessingPipeline()
    pipeline.add_batcher(2, name="test_batcher")
    assert pipeline.end_name == "test_batcher"


def test_ProcessingPipeline_flattener_is_named():
    pipeline = ProcessingPipeline()
    pipeline.add_flattener(name="test_flattener")
    assert pipeline.end_name == "test_flattener"


def test_ProcessingPipeline_describe():
    pipeline = ProcessingPipeline(name="test")
    pipeline.add_transform(lambda x: x ** 2, name="square")
    pipeline.add_loader(ListLoader(), name="square_loader")
    pipeline.add_transform(lambda x: 1 / x, name="reciprocal")
    pipeline.add_loader(ListLoader(), name="square_reciprocal_loader")
    assert pipeline.describe() == ["test", "square", "square_loader", "reciprocal", "square_reciprocal_loader"]


def test_ProcessingPipeline_test_loaders_at_end():
    """Check that the pipeline correctly identifies when there is no loader at the end of a pipeline."""
    pipeline = ProcessingPipeline()
    assert pipeline._loader_at_end() is False
    pipeline.add_transform(lambda x: x ** 2, name="square")
    assert pipeline._loader_at_end() is False
    pipeline.add_transform(lambda x: 1 / x, name="reciprocal")
    assert pipeline._loader_at_end() is False
    pipeline.add_loader(PrintLoader())
    assert pipeline._loader_at_end() is True
    pipeline.add_transform(lambda x: x ** 2, name="square")
    assert pipeline._loader_at_end() is False
    pipeline.add_loader(PrintLoader())
    assert pipeline._loader_at_end() is True


def test_ProcessingPipeline_batch():
    extractor = RecordExtractor([1, 2, 4])
    loader = ListLoader()

    pipeline = ProcessingPipeline()
    pipeline.add_batcher(2, timeout=0.1)
    pipeline.add_loader(loader)

    pipeline.extract(extractor)
    assert loader.records == [(1, 2), (4, )]


def test_ProcessingPipeline_batcher_processes_all_records():
    NUM_RECORDS = 100
    BATCH_SIZE = 90  # a batch size that will result in an incomplete batch

    extractor = RecordExtractor(list(range(0, NUM_RECORDS)))
    loader = ListLoader()
    pipeline = ProcessingPipeline()
    pipeline.add_batcher(BATCH_SIZE, timeout=1)
    pipeline.add_loader(loader)
    pipeline.extract(extractor)

    # these tests may fail when the batcher's timeout is not fast enough to process all records
    # the pipeline extract SHOULD block until all records are processed.
    assert len(loader.records) == 2, "The batcher should have processed all records, resulting in 2 batches"
    assert len(loader.records[0]) == BATCH_SIZE, "The first batch should be full"
    assert len(loader.records[1]) == NUM_RECORDS - BATCH_SIZE, "The second batch should be incomplete"

# test running an empty set of records


def test_ProcessingPipeline_batcher_no_records():
    extractor = RecordExtractor([])
    loader = ListLoader()
    pipeline = ProcessingPipeline()
    pipeline.add_batcher(10, timeout=1)
    pipeline.add_loader(loader)
    pipeline.extract(extractor)
    assert len(loader.records) == 0, "The batcher should not have processed any records"


def test_ProcessingPipeline_transform_only():
    extractor = RecordExtractor([])
    loader = ListLoader()
    pipeline = ProcessingPipeline()
    pipeline.add_loader(loader)
    pipeline.extract(extractor)
    assert len(loader.records) == 0, "Loader should have received no records"


def test_ProcessingPipeline_raises_if_extract_is_run_without_a_loader_present():
    """Test that extracting fails when when there is no loader."""
    pipeline = ProcessingPipeline()
    with pytest.raises(ValueError):
        pipeline.extract(RecordExtractor([1, 2, 3]))


def test_run_pipeline_from_factory():
    """Test that running a pipeline using the PipelineFactory mechanism works."""
    num = 10  # number of records to push through the pipeline
    destination = []  # destination to "load" the records into

    class TestPipelineFactory(PipelineFactory):
        """Factory to create a bundle of data extraction and processing steps."""

        class Options(PipelineFactory.Options):
            num: int = PipelineFactory.Field(default=1, description="Number of records to emit")

        def get_extractor(self, opts: Options) -> Extractor:
            """Return a simple extractor that emits integers."""
            class MyExtractor(Extractor):
                def iter_records(self):
                    for i in range(opts.num):
                        yield i

            return MyExtractor()

        def get_pipeline(self, opts: Options) -> ProcessingPipeline:
            """Return a pipeline that puts the extracted records into a list."""
            pipeline = ProcessingPipeline(name="test_pipeline_factory")
            pipeline.add_loader(ListLoader(destination))
            return pipeline

    run_pipeline(TestPipelineFactory(), TestPipelineFactory.Options(num=num))
    assert destination == list(range(num))
