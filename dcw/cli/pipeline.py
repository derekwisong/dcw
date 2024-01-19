"""Command line interface to discover and run pipelines.

When this module is installed, the `dcw-pipeline` command is available. It can be used to:

- List available pipelines in a module
- Show help for a pipeline
- Run a pipeline

Examples:
    List pipelines in a module. `PATH` is a `module.submodule` containing `dcw.etl.pipeline.PipelineFactory` subclasses:
    ```bash
    $ python -m dcw.cli.pipeline list PATH
    ```
    and
    ```bash
    $ dcw-pipeline list PATH
    ```

    Show help for a pipeline.
    ```bash
    $ python -m dcw.cli.pipeline help mymodule.mysubmodule.SomePipelineFactory
    ```
"""

import sys
import os
import argparse
import logging
import tabulate

from ..etl.pipeline import find_pipeline_factories, get_factory_class_by_path, run_pipeline, logger as etl_pipe_logger
from ..etl.extract import logger as extract_logger
from ..logging import add_console_logging

logger = logging.getLogger(__name__)


def main_list(args: argparse.Namespace) -> None:
    """Entrypoint when the 'list' command is used."""
    path = str(args.path)
    logger.debug(f"Listing pipelines in {path}")
    pipelines = find_pipeline_factories(args.path, recursive=args.recursive)

    if not pipelines:
        print(f"No pipelines found in {path}")
        return

    tbl = ((f"{p.__module__}.{p.__name__}", p.__doc__.strip().splitlines()[0] if p.__doc__ else "") for p in pipelines)
    print("Available pipelines:")
    print(tabulate.tabulate(tbl, headers=("Path", "Description"), tablefmt="grid"))


def main_run(args: argparse.Namespace) -> None:
    """Entrypoint when the 'run' command is used."""
    factory_class = get_factory_class_by_path(args.path)

    # get the CLI argument parser for the pipeline and set the program name to look like the command we're running
    parser = factory_class.create_argument_parser(
        prog=f"{os.path.basename(sys.argv[0])} run [OPTIONS] {args.path} [-- [PIPELINE OPTIONS]]")

    # parse the arguments and convert them to an instance of the Options type for the pipeline
    pipeline_args = parser.parse_args(args.remaining or [])
    logger.debug(f"Parsed pipeline arguments: {pipeline_args}")
    pipeline_opts = factory_class.args_to_opts(pipeline_args)

    if not args.dry_run:
        logger.info(f"Running {factory_class.__name__} with options {pipeline_opts}")
        factory = factory_class()
        for pipeline_logger in factory.get_loggers():
            pipeline_logger.setLevel(logger.level)
            add_console_logging(pipeline_logger)
        run_pipeline(factory, pipeline_opts)
    else:
        logger.info(f"Dry run enabled; would have run {factory_class.__name__} with options {pipeline_opts}")


def main_help(args: argparse.Namespace) -> None:
    """Entrypoint when the 'help' command is used."""
    factory_class = get_factory_class_by_path(args.path)
    parser = factory_class.create_argument_parser()
    parser.usage = argparse.SUPPRESS
    parser.print_help()


def setup_parser() -> argparse.ArgumentParser:
    """Setup the command line argument parser."""
    parser = argparse.ArgumentParser(description="DCW Pipelines.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    subparsers = parser.add_subparsers(required=True, title="commands", dest="command")

    list_parser = subparsers.add_parser("list", help="List available pipeline factories")
    list_parser.add_argument("--recursive", "-r", action="store_true", help="Recursively search for pipelines")
    list_parser.add_argument("path", type=str, help="Module path to pipeline factory(ies)", metavar="PATH")
    list_parser.set_defaults(func=main_list)

    run_parser = subparsers.add_parser(
        "run",
        help="Run a pipeline",
        usage=f"{parser.prog} run [OPTIONS] PATH [-- [PIPELINE OPTIONS]]")

    run_parser.add_argument("path", type=str, help="Path to pipeline factory (module.ClassName)", metavar="PATH")
    run_parser.add_argument("--dry-run", "-d", action="store_true", help="Do not actually run the pipeline.")
    run_parser.add_argument("remaining", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    run_parser.set_defaults(func=main_run)

    help_parser = subparsers.add_parser("help", help="Show help for a pipeline")
    help_parser.add_argument("path", type=str, help="Path to pipeline factory (module.ClassName)", metavar="PATH")
    help_parser.set_defaults(func=main_help)
    return parser


def main(args: list[str] | None = None) -> None:
    """Run the pipeline CLI."""
    logger.setLevel(logging.DEBUG)
    add_console_logging(logger)

    parser = setup_parser()
    args = parser.parse_args(args)

    if not args.verbose:
        logger.setLevel(logging.INFO)

    etl_pipe_logger.setLevel(logger.level)
    add_console_logging(etl_pipe_logger)

    extract_logger.setLevel(logger.level)
    add_console_logging(extract_logger)

    try:
        logger.debug(f"Executing command '{args.command}' using {args.func}")
        args.func(args)
    except Exception:
        logger.exception(f"Unhandled exception executing the '{args.command}' command")


if __name__ == "__main__":
    main()
