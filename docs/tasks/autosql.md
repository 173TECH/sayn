# `autosql` Task

## About

The `autosql` task lets you write a `SELECT` statement and SAYN then automates the data processing (i.e. table or view creation, incremental load, etc.) for you.

## Defining `autosql` Tasks In `models.yaml`

An `autosql` task is defined as follows:

```yaml
task_autosql:
  type: autosql
  file_name: task_autosql.sql
  materialisation: table
  destination:
    tmp_schema: analytics_staging
    schema: analytics_models
    table: task_autosql
```

An `autosql` task is defined by the following attributes:

* `type`: the task type, this needs to be one the the task types supported by SAYN.
* `file_name`: the name of the file **within the sql folder of the project's root**.
* `materialisation`: this should be either `table`, `view` or `incremental`. `table` will create a table, `view` will create a view. `incremental` will create a table and will load the data incrementally based on a delete key (see more detail on `incremental` below).
* `destination`: this sets the details of the data processing.
    * `tmp_schema`: specifies the schema which will be used to store any necessary temporary object created in the process. This is optional.
    * `schema`: is the destination schema where the object will be created. This is optional.
    * `table`: is the name of the object that will be created.
* `delete_key`: specifies the incremental process delete key. This is for `incremental` `materialisation` only.

## Controlling DDLs

If desired, you can control the DDL of the SQL process by using the `ddl` parameter as follows:

```yaml
task_autosql:
  type: autosql
  file_name: task_autosql.sql
  materialisation: table
  destination:
    tmp_schema: analytics_staging
    schema: analytics_models
    table: task_autosql
  ddl:
    #details
```

Here are the lists of available `ddl` options:

* `permissions`: automatically grants permissions on the created object to specified roles and users.
* `primary_key`: sets the primary key on the table.
* `indexes`: sets an index on the table.
* `columns`: enables to specify the column types.

## Using `autosql` In `incremental` Mode

If you do not want to have a full refresh of your tables, you can use the `autosql` task with `incremental` `materialisation`. This is extremely useful for large data volumes when full refresh would be too long.

SAYN `autosql` tasks with `incremental` materialisation require a `delete_key` to be set. Please see below an example:

```yaml
task_autosql_incremental:
  file_name: task_autosql_incremental.sql
  type: autosql
  materialisation: incremental
  to:
    staging_schema: analytics_staging
    schema: analytics_models
    table: task_autosql
  delete_key:
      - dt
```

When using `incremental`, SAYN will do the following in the background:

1. Create a temporary table based on the incremental logic from the SAYN query.
2. Delete rows from the target table that are found in the temporary table based on the `delete_key`.
3. Load the temporary table in the destination table.
