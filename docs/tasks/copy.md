# `copy` Task

## About

The `copy` task copies tables from one database to another. It can be used to automatically
ingest data from operational databases (e.g. PostgreSQL) to your analytics warehouse.

## Defining `copy` tasks

A `copy` task is defined as follows:

!!! example "dags/base.yaml"
    ```yaml
    task_copy:
      type: copy
      source:
        db: from_db
        schema: from_schema
        table: from_table
      destination:
        tmp_schema: staging_schema
        schema: schema
        table: table_name
    ```

`copy` tasks have the following parameters that need to be set:

* `type`: `copy`.
* `source`: the source details
    * `db`: a credential from the `required_credentials` list in `project.yaml` that's one of the supported [databases](../databases/overview.md).
    * `schema`: the (optional) source schema.
    * `table`: the name of the table top copy.
* `destination`: the destination details. The destination database is the `default_db` set in `models.yaml`.
    * `tmp_schema`: the (optional) staging schema used in the process of copying data.
    * `schema`: the (optional) destination schema.
    * `table`: the name of the table to store data into.

By default, tables will be copy in full every time SAYN runs, but it can be changed into an incremental
load by adding `incremental_key` and `delete_key`:

* `incremental_key`: the column to use to determine what data is new. The process will transfer
  any data in the source table with an `incremental_key` value superior to the maximum found in the destination.
* `delete_key`: the column which will be used for deleting data in incremental loads. The process
  will delete any data in the destination table with a `delete_key` value found in the new dataset
  obtained before inserting.

!!! example "dags/base.yaml"
    ```yaml
    task_copy:
      type: copy
      source:
        db: from_db
        schema: from_schema
        table: from_table
      destination:
        tmp_schema: staging_schema
        schema: schema
        table: table_name
      incremental_key: updated_at
      delete_key: id
    ```

In this example, we use `updated_at` which is a field updated every time a record changes (or is created)
on a hypothetical backend database to select new records, and then we replace all records in the target
based on the `id`s found in this new dataset.

## Data types and DDL

`copy` tasks accept a `ddl` field in the task definition in the same way that `autosql` does. With this
specification, we can override the default behaviour of copy when it comes to column types by enforcing
specific column types in the final table:

!!! example "dags/base.yaml"
    ```yaml
    task_copy:
      type: copy
      source:
        db: from_db
        schema: from_schema
        table: from_table
      destination:
        tmp_schema: staging_schema
        schema: schema
        table: table_name
      incremental_key: updated_at
      delete_key: id
      ddl:
        columns:
          - id
          - name: updated_at
            type: timestamp
    ```

In this example we define 2 columns for `task_copy`: `id` and `updated_at`. This will make SAYN:
1. Copy only those 2 columns, disregarding any other columns present at source
2. Infer the type of `id` based on the type of that column at source
3. Enforce the destination table type for `updated_at` to be `TIMESTAMP`

Additionally, in the `ddl` property we can specify indexes and permissions like in `autosql`.
Note that some databases support specific DDL other than these.
