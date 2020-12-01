# `sql` Task

## About

The `sql` task lets you execute a SQL script with one or many statements. This is useful for
executing `UPDATE` statements for example, that wouldn't be covered by `autosql`.

## Defining `sql` tasks

A `sql` task is defined as follows:

!!! example "tasks/base.yaml"
    ```yaml
    task_sql:
      type: sql
      file_name: sql_task.sql
    ```

Where `file_name` is the path to a file under the `sql` folder containing the
SQL script to execute.
