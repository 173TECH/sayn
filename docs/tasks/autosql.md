# `autosql` Task

## About

The `autosql` task lets you write a `SELECT` statement and SAYN then automates the data processing (i.e. table or view creation, incremental load, etc.) for you.

## Defining `autosql` Tasks

An `autosql` task is defined as follows:

!!! example "autosql task definition"
    ```yaml
    ...

    task_autosql:
      type: autosql
      file_name: task_autosql.sql
      materialisation: table
      destination:
        tmp_schema: analytics_staging
        schema: analytics_models
        table: task_autosql
    ...
    ```

An `autosql` task is defined by the following attributes:

* `type`: `autosql`.
* `file_name`: the name of the file **within the sql folder of the project's root**.
* `materialisation`: this should be either `table`, `view` or `incremental`. `table` will create a table, `view` will create a view. `incremental` will create a table and will load the data incrementally based on a delete key (see more detail on `incremental` below).
* `destination`: this sets the details of the data processing.
    * `tmp_schema`: specifies the schema which will be used to store any necessary temporary object created in the process. This is optional.
    * `schema`: is the destination schema where the object will be created. This is optional.
    * `table`: is the name of the object that will be created.
* `delete_key`: specifies the incremental process delete key. This is for `incremental` `materialisation` only.

## Using `autosql` In `incremental` Mode

`autosql` tasks support loads incrementally, which is extremely useful for large data volumes when full
refresh (`materialisation: table`) would be infeasible.

We set an `autosql` task as incremental by:
1. Setting `materialisation` to `incremental`
2. Defining a `delete_key`

!!! example "autosql in incremental mode"
    ```yaml
    ...

    task_autosql_incremental:
      type: autosql
      file_name: task_autosql_incremental.sql
      materialisation: incremental
      destination:
        tmp_schema: analytics_staging
        schema: analytics_models
        table: task_autosql
      delete_key: dt
    ...
    ```

When using `incremental`, SAYN will do the following in the background:

1. Create a temporary table based on the incremental logic from the SAYN query.
2. Delete from the final table those records for which the `delete_key` value is in the temporary table.
3. Insert the contents of the temporary table into the final table.

In order to make the `SELECT` statement incremental, SAYN provides the following arguments:

* `full_load`: a flag defaulting to `False` and controlled by the `-f` flag in the SAYN command.
  If `-f` is passed to the sayn command, the final table will be replaced with the temporary one
  in step 2 above, rather than performing a merge of the data.
* `start_dt`: a date defaulting to "yesterday" and controlled by the `-s` flag in the SAYN command.
* `end_dt`: a date defaulting to "yesterday" and controlled by the `-e` flag in the SAYN command.

!!! example "SQL using incremental arguments"
    ```sql
    SELECT dt
         , field2
         , COUNT(1) AS c
      FROM table
     WHERE dt BETWEEN {{ start_dt }} AND {{ end_dt }}
     GROUP BY 1,2
    ```

## Defining DDLs

Additionally, autosql tasks support the definition of optional DDL that will be used when creating the table.
Each supported database might have specific DDL related to it, but in general the following is supported:

* columns: the list of columns as well as their type. If used, SAYN will enforce the types specified.
* indexes: the indexes to add on the table.
  * primary_key: this should be added in the indexes section using the `primary_key` name for the index.
* permissions: the permissions you want to give to each role. You should map each role to the rights
  you want to grant separated by commas (e.g. SELECT, DELETE).

!!! example "autosql with DDL"
    ```yaml
    ...

    task_autosql:
      type: autosql
      file_name: task_autosql.sql
      materialisation: table
      destination:
        tmp_schema: analytics_staging
        schema: analytics_models
        table: task_autosql
      ddl:
        indexes:
          primary_key:
            columns:
              - column1
              - column2
          idx1:
            columns:
              - column1
      permissions:
        role_name: SELECT
    ...
    ```
