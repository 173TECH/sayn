# Tasks

## About

Tasks are the core components of your SAYN DAGs (Directed Acyclic Graph). Through tasks, SAYN provides a lot of automation under the hood, so make sure you explore the various task types SAYN offers! Tasks will be executed in an order based on the parents you define.

## Defining Tasks In DAGs

Tasks are defined in individual [DAGs](../dags.md). Their definition is composed of several attributes.

### `type`

A task is defined by its `type` - `type` is the only attribute shared by all tasks. Below is a simple example of a SAYN task definition:

**`dag.yaml`**
```yaml
task_1:
  type: sql
  file_name: task_1.sql
```
Please see the `Core Tasks` and `Extension Tasks` sections on this page for the list of all task types.

### `parents`

If a task is dependent upon another task, it can define `parents`. A `task` can have as many `parents` as desired. Please see below an example which shows how to define `parents`:

**`dag.yaml`**
```yaml
task_2:
  type: sql
  file_name: task_2.sql
  parents:
    - task_1
```

### `tags`

Tasks can define a `tags` attribute - you can define as many `tags` as desired on a task. Those `tags` can be used in order to run only `tasks` which are defined with a specific tag. This is useful to group several `tasks` across multiple DAGs under one structure. Please see below how to define `tags` on a task:

**`dag.yaml`**
```yaml
task_3:
  type: python
  class: my_module.MyClass
  tags:
    - extract
```

### `type` specific attributes

Different task types have different attributes. Make sure that you check each task type's specific documentation to understand how to define it.

## Task Types

Please see below the available SAYN task types:

- [`autosql`](autosql.md): simply write a `SELECT` statement and SAYN automates the data processing (i.e. table or view creation, incremental load, etc.) for you.
- [`python`](python.md): enables you to write a Python process. Can be used for a wide range of cases from data extraction to data science models - anything Python lets you do.
- [`copy`](copy.md): enables to automatically copy data from one database to another.
- [`dummy`](dummy.md): those tasks do not do anything. They can be used as connectors between tasks.
- [`sql`](sql.md): executes any SQL statement. There can be multiple statements within the SQL file.
