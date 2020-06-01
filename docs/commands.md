# Commands

## About

SAYN commands are structured as `sayn [command] [additional parameters]`. The best way to check SAYN commands is by running `sayn --help` and `sayn [command] --help` in your command line. Please see below the available SAYN commands.

## Commands Detail

### `sayn init`

Initialises a SAYN project in the current working directory.

### `sayn run`

Runs the whole SAYN project. **This command should be run from the project's root.** It has the following optional flags which can be cumulated as desired:

* `-t`: run specific tasks.
* `-x`: exclude specific tasks.
* `-p`: select profile to use for run.
* `-f`: do a full refresh. Mostly useful on incremental tasks to refresh the whole table.
* `-s`: start date for incremental loads.
* `-e`: end date for incremental loads.
* `-d`: display logs from `DEBUG` level.

#### `sayn run -t`

You can run specific tasks with the following commands:

* `sayn run -t task_name`: run `task_name`
* `sayn run -t +task_name`: run `task_name` and all its parents.
* `sayn run -t task_name+`: run `task_name` and all its children.
* `sayn run -t dag:dag_name`: run all tasks from the dag `dag_name`.
* `sayn run -t tag:tag_name` run all tasks tagged with `tag_name`.

#### `sayn run -x`

You can exclude specific tasks from a run with the `-x` flag. It can be used as follows:

* `sayn run -x task_name`: run all tasks except `task_name`.
* `sayn run -t dag:marketing -x task_name`: run all tasks in the `marketing` DAG except `task_name`.

#### `sayn run -p`

Runs SAYN using a specific profile. It is used with the same logic than for the `-t` flag. Please see below some examples:

* `sayn run -p profile_name`: runs SAYN using the settings of `profile_name`.

### `sayn compile`

Compiles the SAYN code that would be executed. **This command should be run from the project's root.** The same optional flags than for `sayn run` apply.

### `dag-image`

Generates a visualisation of the whole SAYN process. **This command should be run from the project's root.** This requires `graphviz` - both the [software](https://www.graphviz.org/download/){target="\_blank"} and the [Python package](https://pypi.org/project/graphviz/){target="\_blank"}.
