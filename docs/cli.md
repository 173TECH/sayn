# SAYN CLI

## About

SAYN's CLI tool is the main means for interacting with SAYN projects. Use `sayn --help` to see all options.

## Available Commands

### `sayn init`

Initialises a SAYN project in the current working directory with the
[SAYN tutorial](tutorials/tutorial_part1.md).

### `sayn run`

Executes the project. Without arguments it will run all tasks, using the default profile defined in
`settings.yaml`. This default behaviour can be overridden with some arguments:

* `-p profile_name`: use the specified profile instead of the default.
* `-d`: extra information to the screen, including messages from `self.debug` in python tasks.

#### Filtering Tasks

Sometimes we don't want to execute all tasks defined in the project. In these instances we can use the
following arguments to filter:

* `-t task_query`: tasks to include.
* `-x task_query`: exclude specific tasks.

Multiple tasks can be included after the argument, accumulating their values. Note that both `-t` and `-x` can be specified multiple times, resulting in the same outcome.

Examples:

* `sayn run -t task_name`: run `task_name` only.
* `sayn run -t task1 task2`: runs `task1` and `task2` only.
* `sayn run -t task1 -t task2`: runs `task1` and `task2` only. (equivalent to the one above.)
* `sayn run -t +task_name`: run `task_name` and all its ancestors.
* `sayn run -t task_name+`: run `task_name` and all its descendants.
* `sayn run -t group:group_name`: run all tasks specified in the group `group_name`.
* `sayn run -t tag:tag_name` run all tasks tagged with `tag_name`.
* `sayn run -x task_name`: run all tasks except `task_name`.
* `sayn run -t group:marketing -x +task_name`: run all tasks in the `marketing` task group except `task_name` and its ancestors.

Quite often we want to make some changes to a small set of tasks, explore the new results, make some more changes and repeat.
When doing this we might not want to have an up to date copy of all upstream objects and instead we might want to use production
as the source of your models. For this we can use the flag `-u` (`--upstream-prod`) which selects from production for tables
not produced by the currently filtered tasks. Head over to [database objects](database_objects.md#upstream_prod) for more details

Both task filtering and upstream prod arguments can be set using `default_run` in `settings.yaml`. Example:

!!! example "settings.yaml"
    profiles:
      dev:
        default_run: -x group:extract

This example will make it so that running `sayn run` will already exclude the tasks in the group called `extract`.

#### Incremental Tasks Options

SAYN uses 3 arguments to manage incremental executions: `full_load`, `start_dt` and `end_dt`; which can
be overridden with these arguments to `sayn run`:

* `-f`: do a full refresh. Mostly useful on incremental tasks to refresh the whole table (default: False).
* `-s`: start date for incremental loads (default: yesterday).
* `-e`: end date for incremental loads (default: yesterday).

These values are available to sql and autosql tasks as well as python tasks with `self.run_arguments`.
When the `sayn run` command is executed, these values define the `Period` specified in the console.

### `sayn compile`

Works like `run` except it doesn't execute the sql code. The same optional flags than for `sayn run` apply.

### `sayn dag-image`

Generates a visualisation of the whole SAYN process. This requires `graphviz` installed in your
[system](https://www.graphviz.org/download/){target="\_blank"} and the python package, which can be
installed with `pip install "sayn[graphviz]"`.
