# Tasks

## About

Tasks are the backbone of your SAYN project. They are used by SAYN to create a DAG (Directed Acyclic Graph).

!!! info
    A Directed Acyclic Graph is a concept which enables to conveniently model tasks and dependencies. It uses the following key principles

    * `graph`: a specific data structure which consists of `nodes` connected by `edges`.
    * `directed`: dependencies have a direction. If there is an `edge` (i.e. a dependency) between two tasks, one will run before the other.
    * `acyclic`: there are no circular dependencies. If you process the whole graph, you will never encounter the same task twice.

Dependencies between tasks are defined with the `parents` list. To relate back to the DAG concept, this implies each task in SAYN represents a `node` and `edges` are defined by the `parents` attribute of each task. For example, the SAYN tutorial defines the following DAG:

![Tutorial](../dag.png)

Through tasks, SAYN provides a lot of automation under the hood, so make sure you explore the various task types SAYN offers!

## Task Types

Please see below the available SAYN task types:

- [`autosql`](autosql.md): simply write a `SELECT` statement and SAYN automates the data processing (i.e. table or view creation, incremental load, etc.) for you.
- [`python`](python.md): enables you to write a Python process. Can be used for a wide range of cases from data extraction to data science models - anything Python lets you do.
- [`copy`](copy.md): enables to automatically copy data from one database to another.
- [`dummy`](dummy.md): those tasks do not do anything. They can be used as connectors between tasks.
- [`sql`](sql.md): executes any SQL statement. There can be multiple statements within the SQL file.

## Defining Tasks

Tasks are defined in YAML files located under the `tasks` folder at the root level of your SAYN project. Each file in the `tasks` folder represents a [task group](#task_groups) and can be executed independently. By default, SAYN includes any file in the `tasks` folder ending with a `.yaml` extension when creating the DAG.

Within each YAML file, tasks are defined in the `tasks` entry.

!!! example "tasks/base.yaml"
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
| preset | A preset to inherit task properties from. See [the presets section](../presets.md) for more info. | Optional name of preset |
| parents | A list of tasks this one depends on. All tasks in this list is ensured to run before the child task. | Optional list |
| tags | A list of tags used in `sayn run -t tag:tag_name`. This allows for advanced task filtering when we don't want to run all tasks in the project. | Optional list |
| on_fail | Defines the behaviour when the [task fails](#task_failure_behaviour). | Optional one of: `skip` or `no_skip` |

!!! attention
    Different task types have different attributes. Make sure that you check each task type's specific documentation to understand how to define it.

## Task Groups

Task groups are a convenient way to segment and organise your data processes in your SAYN project. Each YAML file in the `tasks` folder represents a task group.

!!! tip
    When growing a SAYN project, it is good practice to start separating your tasks in multiple groups (e.g. extracts, core models, marketing models, finance models, data science, etc.) in order to organise processes.

Each task group file defines tasks (required) and presets (optional).

!!! example "tasks/base.yaml"
    ```yaml
    presets:
      modelling:
        type: autosql
        materialisation: table
        destination:
          tmp_schema: analytics_staging
          schema: analytics_models
          table: '{{task.name}}'

    tasks:
      load_data:
        type: python
        class: load_data.LoadData

      #task defined without preset
      dim_tournaments:
        type: autosql
        file_name: dim_tournaments.sql
        materialisation: table
        destination:
          tmp_schema: analytics_staging
          schema: analytics_models
          table: dim_tournaments
        parents:
          - load_data

      #task defined using a preset
      dim_arenas:
        preset: modelling
        file_name: dim_arenas.sql
        parents:
          - load_data
    ```

| Property | Description | Required |
| -------- | ----------- | -------- |
| tasks | The set of tasks that compose the task group. For more details on `tasks`, please see the [Tasks](overview.md) section. | Yes |
| presets | Defines preset task structures shared by several tasks. Presets defined within task group files can inherit from presets defined at the project level in `project.yaml`. See the [Presets](../presets.md) section for more details. | Optional |

## Task Attributes

Task attributes can be used when defining tasks in a dynamic way. The following example shows how to use the task name and task group dynamically when defining a task:

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      sql_task:
        type: sql
        file_name: '{{task.group}}/{{task.name}}.sql'
    ```

This will effectively tell the task to look for a file located at `base/sql_task.sql` in the `sql` folder.

## Task failure behaviour

When a task fails during an execution, all descendent tasks will be skipped as expected. However sometimes it can be useful to 
execute descending tasks even if a parent fails, for example when an API can frequently throw errors and we want to continue the
execution just with as much data as it was possible to pull from it. In this case we make use of the `on_fail` task property to
specify that we do not want to skip descending tasks.


!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      could_fail_task:
        type: python
        class: could_fail.CouldFailTask
        on_fail: no_skip

      child_task:
        type: sql
        file_name: query_using_could_fail_data.sql
        parents:
          - failing_task
    ```

In the above case, if `could_fail_task` fails, `child_task` will not be skipped.
