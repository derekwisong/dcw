"""Demonstration of a pipeline.

Using the `PipelineFactory` class, you can define a pipeline that can be run from the command line or via code.

Examples:
    List the pipelines in the demo dataset:
    ```bash
    $ dcw-pipeline list dataset.demo

    Available pipelines:
    ╒══════════════════════════════╤════════════════════════════════════════════════════════════════════════════╕
    │ Path                         │ Description                                                                │
    ╞══════════════════════════════╪════════════════════════════════════════════════════════════════════════════╡
    │ dataset.demo.demo.MyPipeline │ Extract sequential numbers, square them, and load the result to a mock db. │
    ╘══════════════════════════════╧════════════════════════════════════════════════════════════════════════════╛
    ```

    Run the pipeline using the `dcw-pipeline` command line tool:
    ```bash
    $ dcw-pipeline run dataset.demo.demo.MyPipeline -- --limit 5
    2024-01-17T10:23:00-0500 dcw.cli.pipeline INFO: Running MyPipeline with options limit=5 (pipeline.py:56)
    Loaded 0
    Loaded 1
    Loaded 4
    Loaded 9
    Loaded 16
    Flushed the loader
    ```

    Run the pipeline from code:
    >>> from dataset.demo import MyPipeline
    >>> from dcw.etl import run_pipeline
    >>> pipeline = MyPipeline()
    >>> options = MyPipeline.Options(limit=5)
    >>> run_pipeline(pipeline, options)
    Loaded 0
    Loaded 1
    Loaded 4
    Loaded 9
    Loaded 16
    Flushed the loader
"""

from typing import Iterator
import dcw.etl


class MyExtractor(dcw.etl.Extractor):
    """An extractor that yields integers from `0` to `limit`."""

    def __init__(self, limit: int):
        self.limit = limit

    def iter_records(self) -> Iterator[int]:
        for i in range(self.limit):
            yield i


class MockDatabaseLoader(dcw.etl.Loader):
    """A mock database loader that only prints what it was asked to load."""

    def load(self, record):
        print(f"Loaded {record}")  # or insert into a database, etc.

    def flush(self):
        print("Flushed the loader")  # or commit a transaction, etc.


class MyPipeline(dcw.etl.PipelineFactory):
    """Extract sequential numbers, square them, and load the result to a mock db."""

    class Options(dcw.etl.PipelineFactory.Options):
        limit: int

    def get_extractor(self, opts: Options) -> dcw.etl.Extractor:
        return MyExtractor(opts.limit)

    def get_pipeline(self, opts: Options) -> dcw.etl.ProcessingPipeline:
        pipeline = dcw.etl.ProcessingPipeline()
        pipeline.add_transform(lambda x: x ** 2)
        pipeline.add_loader(MockDatabaseLoader())
        return pipeline


def main():
    factory = MyPipeline()
    options = MyPipeline.Options(limit=10)
    dcw.etl.run_pipeline(factory, options)


if __name__ == "__main__":
    main()
