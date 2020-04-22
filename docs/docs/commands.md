# Commands

## About

SAYN commands are structured as `sayn [command] [additional parameters]`. The best way to check SAYN commands is by running `sayn --help` and `sayn [command] --help` in your command line. Please see below the available SAYN commands.

## Commands Detail

* `init`: initialises a SAYN project in the current working directory.
* `run`: runs all tasks if no parameter is specified. This command has the following flags.
    * `-t`: run specific task(s). It should be used as `sayn run -t task_1`. You can run the task and all its parents by prefixing the task name with `+` such as `sayn run -t +task_1`. The same applies for all children with `sayn run -t task_1+`.
    * `-m`: run specific model. This can be used when having multiple model files: `sayn run -m marketing`.
    * `-p`: select profile to use for run
    * `-f`: do a full load. Mostly useful on incremental tasks to refresh the whole table.
    * `-s`: start date for incremental loads.
    * `-e`: end date for incremental loads.
    * `-d`: display logs from `DEBUG` level.
* `compile`: compiles the SAYN code. The code is available in a `compile` folder at the project's root.
    * same parameters apply than the `run` command.
* `dag-image`: generates a visualisation of the DAG. This requires `graphviz` (both the software and the Python package).
