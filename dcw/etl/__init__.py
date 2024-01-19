"""Support for Extract, Transform, and Load (ETL) operations.

## Overview

This module provides a framework for performing ETL operations on data.

1. Extract: Extract data from a source.
2. Transform: Transform the data in some way.
3. Load: Load the data into a destination.

The steps are approximated by the following classes:

- `Extractor`: Base class that defines an interface for extracting data from a source. Used to iterate over the records
  in the source. Users are free to implement their own notion of what a "record" is. For example, a record could be a
  file path in the case of a Extractor that walks a directory tree and outputs file paths. Or a record could be a
  dataframe or table extracted from one or more csv files. Or, a record could be a single row of a csv. The choice is
  up to the user and how they want to structure their processing steps.
- `ProcessingPipeline`: Used to define of a series of operations (transformations, flow control, load/storage) which are
  applied to input data.
- `Loader`: Base class that defines an interface for loading records into a destination. The destination could be a
  file, a database, or some other storage mechanism.
- `Transformation`: Base class that defines an interface for transforming data. Transformations can also be provided as
  simple functions or other callables. These are expected to be operations that take a record as input and produce
  a record as output.

## Usage

A `ProcessingPipeline` can be used in one of two ways:

1. Push: Data is pushed into the pipeline using the `push` method. This is useful when the data is already available
    and can be pushed into the pipeline. One downside of using `push` to run the pipeline is that in the case of flow
    control steps, a call to `push` will return immediately. However, processing may be deferred until later, such as in
    the case of a batcher, which accumulates records, being added to the pipeline. See: `add_batcher`
2. Extract: Data is extracted from a source using the `extract` method. This is useful when the data is not already
    available and must be extracted from some source (using an `Extractor`). When using
    `ProcessingPipeline.extract(my_extractor)` the operation will not return until all data has been processed.

Additionally, The `PipelineFactory` class can be used to bundle the source of data, the processing pipeline, and any
required and optional parameters into a single place. Organizing pipelines this way enables built-in support to run them
using the `dcw-pipeline` command line tool tool or the `run_pipeline` function.


Examples:
    A simple `ProcessingPipeline` that squares input numbers and loads them into a mock database:
    >>> from dcw.etl import ProcessingPipeline
    >>> from dcw.etl.load import ListLoader
    >>> my_database = []
    >>> loader = ListLoader(my_database)
    >>> pipeline = ProcessingPipeline()
    >>> pipeline.add_transform(lambda x: x ** 2)
    >>> pipeline.add_loader(loader)
    >>> pipeline.push(5)
    >>> pipeline.push(10)
    >>> my_database
    [25, 100]

    A `ProcessingPipeline` that includes batching records before loading them into a mock database:
    >>> from dcw.etl import ProcessingPipeline
    >>> from dcw.etl.load import ListLoader
    >>> import time
    >>> my_database = []
    >>> loader = ListLoader(my_database)
    >>> pipeline = ProcessingPipeline()
    >>> pipeline.add_transform(lambda x: x ** 2)
    >>> pipeline.add_batcher(2, timeout=0.5)
    >>> pipeline.add_loader(loader)
    >>> for i in range(5):
    ...     pipeline.push(i)
    >>> time.sleep(0.8)  # sleep to allow the batcher to flush
    >>> my_database
    [(0, 1), (4, 9), (16,)]

    A factory that creates elements of a data processing pipeline which produces data in the form of sequential numbers,
    squares them, and loads them into a mock database which simply prints the output:
    >>> from dcw.etl import Extractor, Loader, PipelineFactory, run_pipeline
    >>> class NumberExtractor(Extractor):
    ...     def __init__(self, end):
    ...         self.end = end
    ...     def iter_records(self):
    ...         for i in range(self.end):
    ...             yield i
    >>> class PrintLoader(Loader):
    ...     def load(self, record):
    ...         print(record)
    >>> class MyPipelineFactory(PipelineFactory):
    ...     class Options(PipelineFactory.Options):
    ...         end: int = PipelineFactory.Field(default=10, description="The number to count to")
    ...     def get_pipeline(self, options):
    ...         pipeline = ProcessingPipeline()
    ...         pipeline.add_transform(lambda x: x ** 2)
    ...         pipeline.add_loader(PrintLoader())
    ...         return pipeline
    ...     def get_extractor(self, options):
    ...         return NumberExtractor(options.end)
    >>> run_pipeline(MyPipelineFactory(), MyPipelineFactory.Options(end=5))
    0
    1
    4
    9
    16

    Using `dcw-pipeline` to run `MyPipelineFactory` (the `--` is required to separate options for `dcw-pipeline` from
    the those for the pipeline itself):
    ```bash
    $ dcw-pipeline run mymodule.mysubmodule.MyPipelineFactory -- --end 5
    0
    1
    4
    9
    16
    ```

"""

from .pipeline import ProcessingPipeline, PipelineFactory, run_pipeline
from .extract import Extractor
from .load import Loader
from .transform import Transformation

__all__ = [
    ProcessingPipeline.__name__,
    PipelineFactory.__name__,
    run_pipeline.__name__,
    Extractor.__name__,
    Loader.__name__,
    Transformation.__name__
]
