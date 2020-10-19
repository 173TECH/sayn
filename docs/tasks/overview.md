# Tasks

## About

Tasks are the core components of your SAYN DAGs (Directed Acyclic Graph). Through tasks, SAYN provides a lot of automation under the hood, so make sure you explore the various task types SAYN offers! Tasks will be executed in an order based on the parents you define.

## Defining Tasks

Tasks are defined in [DAG](../dags.md) files under the `tasks` entry.

!!! example "dags/base.yaml"
    ```yaml
    tasks:
      task_1:
        # Task properties

      task_2:
        # Task properties

      # ...
    ```

All tasks share a number of common properties available:

| Property | Description | Required |
| -------- | ----------- | ---- |
| type | The task type. | Required one of: `autosql`, `sql`, `python`, `copy`, `dummy` |
| preset | A preset to inherit task properties from. Seee [the presets sectinon](../presets.md) for more info. | Optional name of preset |
| parents | A list of tasks this one depends on. All tasks in this list is ensured to run before the child task. | Optional list |
| tags | A list of tags used in `sayn run -t tag:tag_name`. This allows for advanced task filtering when we don't want to run all tasks in the project. | Optional list |

### `type` specific attributes

Different task types have different attributes. Make sure that you check each task type's specific documentation to understand how to define it.

## Task Types

Please see below the available SAYN task types:

- [`autosql`](autosql.md): simply write a `SELECT` statement and SAYN automates the data processing (i.e. table or view creation, incremental load, etc.) for you.
- [`python`](python.md): enables you to write a Python process. Can be used for a wide range of cases from data extraction to data science models - anything Python lets you do.
- [`copy`](copy.md): enables to automatically copy data from one database to another.
- [`dummy`](dummy.md): those tasks do not do anything. They can be used as connectors between tasks.
- [`sql`](sql.md): executes any SQL statement. There can be multiple statements within the SQL file.
