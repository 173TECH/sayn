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

* `file_name`: path to a file under the `sql` folder containing the SQL script to execute.
* `db`: the (optional) destination database.

!!! info
    You do not need to specify `db` unless you want the destination database to be different than the `default_db` you define in `project.yaml` (which is the default database used by SAYN). If you define the `db` attribute, it needs to:

      * Be a credential from the `required_credentials` list in `project.yaml`.
      * Be defined in your `settings.yaml`.
      * Be one of the supported [databases](../databases/overview.md).
