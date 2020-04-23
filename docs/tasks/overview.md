# Tasks

## About

Tasks are the core components of your SAYN project's DAG (directed acyclic graph). Through tasks, SAYN provides a lot of automation under the hood, so make sure you explore the various task types SAYN offers! Tasks will be executed in an order based on their parentage which you define within `models.yaml`.

## Core Tasks

Please see below the core SAYN tasks:

- `autosql`: simply write a `SELECT` statement and SAYN automates the data processing (i.e. table or view creation, incremental load, etc.) for you.
- `python`: enables you to use Python. Can be used for a wide range of cases (basically anything Python lets you do), from data extraction to data science models.

## Extension Tasks

Please see below the other tasks SAYN has available:

- `copy`: enables to automatically copy data from one database to another.
- `dummy`: those tasks do not do anything. They can be used as connectors between tasks.
- `sql`: executes any SQL statement. There can be multiple statements within the SQL file.

## Defining Tasks In `models.yaml`

A task is defined by its `type` (`type` is the only attribute shared by all tasks). Below is a simple example of a SAYN task definition:

```yaml
task_1:
  file_name: task_1.sql
  type: sql
```

If a task is dependent upon another task, it can define `parents` as follows (a task can have as many parent(s) as desired):

```yaml
task_2:
  file_name: task_2.sql
  type: sql
  parents:
    - task_1
```

Finally, the different types of tasks have different attributes that need to be defined. Please see the additional detail in each specific task type's documentation.
