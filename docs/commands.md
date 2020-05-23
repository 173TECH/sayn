# Commands

## About

SAYN commands are structured as `sayn [command] [additional parameters]`. The best way to check SAYN commands is by running `sayn --help` and `sayn [command] --help` in your command line. Please see below the available SAYN commands.

## Commands Detail

### `sayn init`

Initialises a SAYN project in the current working directory.

### `sayn run`

Runs the whole SAYN project. This command has the following optional flags which can be cumulated as desired:

* `-t`: run specific tasks.
* `-p`: select profile to use for run.
* `-f`: do a full load. Mostly useful on incremental tasks to refresh the whole table.
* `-s`: start date for incremental loads.
* `-e`: end date for incremental loads.
* `-d`: display logs from `DEBUG` level.

### `sayn run -t`

Runs the task named `task_name`. In addition, SAYN enables you to run specific tasks in various useful ways:

* `sayn run -t +task_name`: run `task_name` and all its parents.
* `sayn run -t task_name+`: run `task_name` and all its children.
* `sayn run -t dag:dag_name`: run all tasks from the dag `dag_name`.
* `sayn run -t tag:tag_name` run all tasks with tagged with `tag_name`.

### `sayn run -p`

Runs SAYN using the `prod` profile.

### `sayn compile`

Compiles the SAYN code that would be executed. The same optional flags than for `sayn run` apply.

### `dag-image`

Generates a visualisation of the whole SAYN process. This requires `graphviz` (both the software and the Python package).
