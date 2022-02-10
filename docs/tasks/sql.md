# `sql` Task

## About

The `sql` task lets you execute a SQL script with one or many statements. This is useful for
executing `UPDATE` statements for example, that wouldn't be covered by `autosql`.

## Defining `sql` Tasks

A `sql` task is defined as follows:

!!! example "tasks/base.yaml"
    ```yaml
    task_sql:
      type: sql
      file_name: sql_task.sql
    ```

A `sql` task is defined by the following attributes:

* `file_name`: the path to a file **within the sql folder of the project's root**. When defining `sql` `groups` in `project.yaml` this property needs to be a glob expression, for example `group/*.sql`.
* `db`: the (optional) destination database.

!!! info
    By default the task is executed in the database defined by `default_db` in `project.yaml`. `db` can be specified to change this, in which case the connection specified needs to:

      * Be a credential from the `required_credentials` list in `project.yaml`.
      * Be defined in your `settings.yaml`.
      * Be one of the supported [databases](../databases/overview.md).

!!! tip
    The sql code can use the `src` and `out` macros to implicitly create task dependencies as decribed in [database objects](../database_objects.md).

## Config macro

Like `autosql` tasks, we can overload some values specified in the YAML. This is useful when we define
groups in `project.yaml` and for a specific task we need to make a configuration change like the `tags`. To use this we simply call `config` from a Jinja tag within the sql file:

!!! example "autosql with config"
    ```
    {{ config(tags=['creation_tasks']) }}
    
    CREATE TABLE ...
    ```

The above code will override the value of `tags` setting defined in YAML so we can filter on a group of tasks when running SAYN. Other
properties are available for overloading for advanced use cases: `parents`, `outputs` and `sources`.
