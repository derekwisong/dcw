# Data Collection and Wrangling

The `dcw` module provides a framework for collecting and wrangling data.

!!! warning
    This is `v0.x.x` work-in-progress. There are large, often backwards incompatible, changes between releases.

## Installation

```bash
$ pip install dcw
```

### Running Data Pipelines

Pipelines can be run in one of two ways:

1. The `dcw-pipeline` CLI tool can be used to run data pipelines expressed using the `dcw` framework.
2. The [`dcw.etl.pipeline.run_pipeline`](reference/dcw/etl/pipeline.md#dcw.etl.pipeline.run_pipeline) function may be used
   from your own application.

!!! note
    When running these commands while developing, prefix the commands with `poetry run`.

    E.g. `poetry run dcw-pipeline -h`

```bash
$ dcw-pipeline -h
usage: dcw-pipeline [-h] [--verbose] {list,run,help} ...

DCW Pipelines.

options:
  -h, --help       show this help message and exit
  --verbose, -v    Enable verbose logging

commands:
  {list,run,help}
    list           List available pipeline factories
    run            Run a pipeline
    help           Show help for a pipeline
```

A demo pipeline may be found at `dcw.etl.demo_pipeline`:

**List the pipelines in a module:**

```bash
$ dcw-pipeline list dcw.etl.demo_pipeline
Available pipelines:
+----------------------------------+----------------------------------------------------------------------------+
| Path                             | Description                                                                |
+==================================+============================================================================+
| dcw.etl.demo_pipeline.MyPipeline | Extract sequential numbers, square them, and load the result to a mock db. |
+----------------------------------+----------------------------------------------------------------------------+
```

**Show the help message and parameters for a pipeline:**

```bash
$ dcw-pipeline help dcw.etl.demo_pipeline.MyPipeline
Extract sequential numbers, square them, and load the result to a mock db.

options:
  -h, --help     show this help message and exit
  --limit LIMIT
```

**Run a pipeline from the command line and provide arguments:**

```bash
$ dcw-pipeline run dcw.etl.demo_pipeline.MyPipeline -- --limit 10
2024-01-19T09:49:36-0500 dcw.cli.pipeline INFO: Running MyPipeline with options limit=10 (pipeline.py:67)
Loaded 0
Loaded 1
Loaded 4
Loaded 9
Loaded 16
Loaded 25
Loaded 36
Loaded 49
Loaded 64
Loaded 81
Flushed the loader
```

## Developing

This Python project is managed using [poetry](https://python-poetry.org/), tested using `pytest`, and documented using
`mkdocs`.

### Create Virtual Environment and Run Tests

Create a virtual environment and install all dependencies into it:

```bash
$ poetry install
```

Run tests inside the virtual environment:

```bash
$ poetry run pytest
```

### Building

Poetry can be used to build a wheel and source distribution:

```bash
$ poetry build
```

### Serve Documentation

```bash
$ poetry run mkdocs serve
```

Documentation files can be built using `poetry run mkdocs build`.
