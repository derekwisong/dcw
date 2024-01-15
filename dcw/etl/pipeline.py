"""Components and untilities for building data processing pipelines.
"""

import collections
import importlib
import abc
import argparse
import inspect
import logging
from types import ModuleType
import typing
import threading
from collections import deque
from typing import Any, Callable, Iterable, Optional

import pydantic

import streamz

from dcw.etl.extract import Extractor
from dcw.etl.load import Loader

logger = logging.getLogger(__name__)


class DataPipeline:
    """DataPipeline can be used to process data streams.

    The DataPipeline class supports the extract-transform-load (ETL) design pattern. Data can be pushed into the
    pipeline, where it moves through a series of transformations to a loader that does something with the data.

    Multiple loaders can be placed into the pipeline. This may be useful when it is desirable to store data at multiple
    points, or to multiple destinations, during its transformation. For example, a pipeline may have a loader that
    stores data in a database, and another loader that stores data in a file. Or, a pipeline may have a loader that
    stores raw data before applying transformations, and another loader that stores the transformed data.

    Batchers can be added to the pipeline to aggregate data into batches before emitting downstream for further
    transformation or loading. This may be useful when data arrives very fast and it is desirable to process it in bulk
    rather than one record at a time. Or, if a transformation is expensive, it may be desirable to batch records to
    reduce the number of times that transformation is called.

    Flatteners can be added to the pipeline to flatten batches of data into individual records that will be emitted
    downstream.

    Examples:
        Create a pipeline that squares the input data and loads it into a list:
        >>> from dcw.etl.pipeline import DataPipeline
        >>> from dcw.etl.load import ListLoader
        >>> records = []
        >>> pipeline = DataPipeline()
        >>> pipeline.add_transform(lambda x: x ** 2)
        >>> pipeline.add_loader(ListLoader(records))
        >>> pipeline.push(2)
        >>> records
        [4]

        Multiple loaders with batching and flattening:
        >>> from dcw.etl.pipeline import DataPipeline
        >>> from dcw.etl.load import ListLoader
        >>> batched = []
        >>> flattened = []
        >>> pipeline = DataPipeline()
        >>> pipeline.add_batcher(3)
        >>> pipeline.add_loader(ListLoader(batched))
        >>> pipeline.add_flattener()
        >>> pipeline.add_loader(ListLoader(flattened))
        >>> pipeline.push(1)
        >>> pipeline.push(2)
        >>> pipeline.push(3)
        >>> batched
        [(1, 2, 3)]
        >>> flattened
        [1, 2, 3]


    Attributes:
        name (str): Name of the pipeline.
        end_name (str): Name of the last point in the pipeline.
    """
    current: streamz.Stream
    source: streamz.Stream

    def __init__(self, name: str = None) -> None:
        """Create a new DataPipeline.

        Arguments:
            name (str): Name of the pipeline.
        """
        self.source = streamz.Stream(stream_name=name)
        self.current = self.source

    @property
    def name(self):
        return self.source.name

    @property
    def end_name(self):
        return self.current.name

    def push(self, data: Any, callback: Callable = None) -> None:
        """Push data into the pipeline.

        Arguments:
            data (Any): Data that will be pushed into the pipeline.
            callback (Optional[Callable]): Callback that will be invoked when the data has been processed.
        """
        metadata = [{"ref": streamz.RefCounter(cb=callback)}] if callback is not None else None
        self.source.emit(data, metadata=metadata)

    def _loader_at_end(self) -> bool:
        """Check if the pipeline has a loader at the end.

        Returns:
            bool: True if the pipeline has a loader at the end, False otherwise.
        """
        return len(self.current.downstreams) > 0

    def extract(self, extractor: Extractor) -> None:
        """Extract records and push them into the pipeline.

        This method blocks until all records have been processed through the pipeline.

        After all records have been processed, the `Loader` instances in pipeline will be flushed using their `flush()`
        methods.

        Arguments:
            extractor (Extractor): Extractor instance that will be used to extract records.
        """
        if not self._loader_at_end():
            raise ValueError("Pipeline must have a loader at the end")

        cv = threading.Condition()
        tasks = 0

        def callback():
            nonlocal tasks
            with cv:
                tasks -= 1
                cv.notify()

        for record in extractor.iter_records():
            self.push(record, callback=callback)

            with cv:
                tasks += 1
                cv.notify()

        while tasks > 0:
            with cv:
                cv.wait_for(lambda: tasks <= 0)

        # flush all the loaders
        self.flush_loaders()

    def add_transform(self, func: Callable[[Any], Any], *, name: Optional[str] = None) -> None:
        """Add a transformation to the pipeline.

        Arguments:
            func (Callable): Callable that will be used to transform the data.
            name (Optional[str]): Name of the transformation.
        """
        self.current = self.current.map(func, stream_name=name)

    def add_batcher(self, size: int, *, name: Optional[str] = None, timeout: Optional[float] = None) -> None:
        """Add a batcher to the pipeline.

        The batcher will build up batches of some size and emit them downstream.

        Arguments:
            size (int): Size of the batches.
            name (Optional[str]): Name of the batcher.
            timeout (Optional[float]): Timeout in seconds after which the batcher will emit a partial batch.
        """
        self.current = self.current.partition(size, timeout=timeout, stream_name=name)

    def add_flattener(self, *, name: Optional[str] = None) -> None:
        """Add a flattener to the pipeline.

        The flattener will flatten batches of data into records that will be emitted downstream.

        Arguments:
            name (Optional[str]): Name of the flattener.
        """
        self.current = self.current.flatten(stream_name=name)

    def add_loader(self, loader: Loader, *, name: Optional[str] = None) -> None:
        """Add a loader to the pipeline.

        A Loader is used to store data somewhere. This may be a database, or a file, or a list, or something else.

        Pushing data into a DataPipeline that doesn't have a loader will result in the data never being processed, but
        left sitting in the pipeline waiting for a loader to be added. Think of loader as a sink that is pulling data
        through the pipeline.

        Arguments:
            loader (Loader): Loader instance that will be used to load data.
            name (Optional[str]): Name of the loader.
        """
        self.current.sink(loader, stream_name=name)

    def describe(self):
        visited = []
        queue = deque()
        queue.append(self.source)

        while queue:
            stream = queue.popleft()
            visited.append(stream.name)
            for child in stream.downstreams:
                queue.append(child)

        return visited

    def get_loaders(self) -> Iterable[Loader]:
        """Get the loaders in the pipeline.

        Returns:
            Iterable[Loader]: Iterable of Loader instances.
        """
        queue = deque()
        queue.append(self.source)

        while queue:
            stream = queue.popleft()
            if 0 == len(stream.downstreams):
                yield stream.func
            for child in stream.downstreams:
                queue.append(child)

    def flush_loaders(self) -> None:
        logger.debug("Flushing loaders")
        for loader in self.get_loaders():
            logger.debug(f"Flushing loader {loader}")
            loader.flush()


class PipelineFactory(abc.ABC):
    """A factory for building everything you need to run a data processing pipeline.

    This class is intended to make it easy to build a modular end-to-end data processing pipeline. A pipeline expressed
    through this factory class can be run from the command line, a script, or even a notebook.

    Users should implement the `get_pipeline` and `get_extractor` methods and, if desired, define an `Options` subclass.

    The `Options` subclass is intended to be used to define the options that will be used to configure the pipeline and
    extractor.

    Examples:
        A full pipeline which can be run from the command line or another script:
        >>> from dcw.etl.load import ListLoader
        >>> from dcw.etl.pipeline import DataPipeline, PipelineFactory, run_pipeline
        >>> from dcw.etl.extract import Extractor
        >>>
        >>> class MyExtractor(Extractor):
        ...     def __init__(self, num):
        ...         self.num = num
        ...
        ...     def iter_records(self):
        ...         for i in range(self.num):
        ...             yield i
        >>>
        >>> class DemoPipelineFactory(PipelineFactory):
        ...     class Options(PipelineFactory.Options):
        ...         num: int = PipelineFactory.Field(default=1, description="Number of records to emit")
        ...
        ...     def get_extractor(self, opts: Options) -> Extractor:
        ...         return MyExtractor(opts.num)
        ...
        ...     def get_pipeline(self, opts: Options) -> DataPipeline:
        ...         # return a data pipeline that simply loads squared records into a list
        ...         pipeline = DataPipeline()
        ...         pipeline.add_transform(lambda x: x ** 2)
        ...         pipeline.add_loader(ListLoader(destination))
        ...         return pipeline
        >>>
        >>> num = 10  # number of records to push through the pipeline
        >>> destination = []  # destination to "load" the records into
        >>> run_pipeline(DemoPipelineFactory(), DemoPipelineFactory.Options(num=10))
        >>> destination
        [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
    """

    class Options(pydantic.BaseModel):
        pass

    def get_loggers(self) -> Iterable[logging.Logger]:
        """Get the loggers that will be used by the pipeline.

        Returns:
            logging.Logger: Logger instance.
        """
        yield

    @abc.abstractmethod
    def get_pipeline(self, opts: Options) -> DataPipeline:
        """Get the data processing pipeline that will be used to process extracted data.

        Arguments:
            opts (Options): Options instance.

        Returns:
            DataPipeline: DataPipeline instance.
        """
        pass

    @abc.abstractmethod
    def get_extractor(self, opts: Options) -> Extractor:
        """Get the extractor that will produce data to be processed by the pipeline.

        Arguments:
            opts (Options): Options instance.

        Returns:
            Extractor: Extractor instance.
        """
        pass

    @classmethod
    def Field(cls, *args, **kwargs):
        # TODO: create own abstraction for fields. this is a hack to avoid requiring users to import pydantic
        # in order to add fields to their options
        return pydantic.Field(*args, **kwargs)

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add arguments to an ArgumentParser.

        Arguments:
            parser (argparse.ArgumentParser): The parser to add arguments to.
        """
        for field, model in cls.Options.model_fields.items():
            is_list = typing.get_origin(model.annotation) is list
            default = model.get_default() if not model.is_required() else ([] if is_list else None)
            parser.add_argument(
                f"--{field}",
                required=model.is_required(),
                default=default,
                nargs="+" if is_list else None,
                help=model.description)

    @classmethod
    def create_argument_parser(cls, prog: str = None) -> argparse.ArgumentParser:
        """Create an ArgumentParser for the factory.

        Arguments:
            prog (Optional[str]): The program name to use for the parser. Default: `sys.argv[0]`.

        Returns:
            argparse.ArgumentParser: The ArgumentParser for the factory.
        """
        parser = argparse.ArgumentParser(prog=prog, description=cls.__doc__)
        cls.add_arguments(parser)
        return parser

    @classmethod
    def args_to_opts(cls, args: argparse.Namespace) -> Options:
        """Convert arguments taken from the command line to an Options instance.

        Arguments:
            args (argparse.Namespace): Arguments taken from the command line.

        Returns:
            Options: Options instance.
        """
        return cls.Options(**vars(args))


def run_pipeline(factory: PipelineFactory, opts: PipelineFactory.Options | None = None) -> None:
    """Run a pipeline using the given factory and options.

    The factory will be used to create the pipeline and extractor, and then the pipeline will be used to process all
    records produced by the extractor, by calling `pipeline.extract(extractor)`.

    Args:
        factory: The factory to use to create the pipeline and extractor.
        opts: Optional. The options to use to create the pipeline and extractor. If not provided, the default options
            will be constructed using the `PipelineFactory.Options` class.
    """
    if opts is None:
        opts = factory.Options()

    pipeline = factory.get_pipeline(opts)
    extractor = factory.get_extractor(opts)
    logger.debug(f"Starting extraction for '{pipeline.name}'")
    pipeline.extract(extractor)
    logger.debug(f"Pipeline extraction for '{pipeline.name}' finished")


def get_factory_class_by_path(path: str) -> type[PipelineFactory]:
    """Get a PipelineFactory by name.

    Args:
        path: The "module.submodule.ClassName" path of the PipelineFactory.

    Returns:
        The PipelineFactory class with the given path.

    Raises:
        ValueError: If the path is not to a PipelineFactory
    """
    class_path = path.split(".")

    if len(class_path) < 2:
        raise ValueError(f"Invalid path {path}, expecting: module.submodule.ClassName")

    try:
        module = importlib.import_module(".".join(class_path[:-1]))
    except Exception as e:
        raise ImportError(f"Unable to import {path}") from e

    factory_class = getattr(module, class_path[-1])

    if not issubclass(factory_class, PipelineFactory):
        raise ValueError(f"{factory_class} is not a PipelineFactory")

    return factory_class


def find_pipeline_factories(module: str | ModuleType, *, recursive: bool = False) -> list[type[PipelineFactory]]:
    """Find all PipelineFactory subclasses in the module.

    Args:
        module: The module to search.
        recursive: Whether to recursively search submodules.

    Returns:
        A list of PipelineFactory subclasses in the module.
    """
    if isinstance(module, str):
        try:
            module = importlib.import_module(module)
        except Exception as e:
            raise ImportError(f"Could not import module {module}") from e

    subclasses = set([])
    modules = collections.deque([module])

    while len(modules) > 0:
        module = modules.popleft()
        members = inspect.getmembers(
            module,
            predicate=lambda x: inspect.isclass(x) and issubclass(x, PipelineFactory) and x != PipelineFactory)
        subclasses.update(subclass_obj for _, subclass_obj in members)

        if recursive:
            submodules = inspect.getmembers(
                module,
                predicate=lambda x: inspect.ismodule(x) and x.__name__.startswith(module.__name__))
            modules.extend(submodule_obj for _, submodule_obj in submodules)

    return sorted(subclasses, key=lambda x: x.__name__)
