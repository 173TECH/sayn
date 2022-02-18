# `copy` Task

## About

The `copy` task copies tables from one database to another. It can be used to automatically
ingest data from operational databases (e.g. PostgreSQL) to your analytics warehouse.

!!! attention
    Copy tasks can only be defined in YAML groups in the tasks folder, not directly in `project.yaml`.

## Defining `copy` Tasks

A `copy` task is defined as follows:

!!! example "tasks/base.yaml"
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
    * `db`: the source database.
    * `schema`: the (optional) source schema.
    * `table`: the name of the table top copy.
* `destination`: the destination details.
    * `tmp_schema`: the (optional) staging schema used in the process of copying data.
    * `schema`: the (optional) destination schema.
    * `table`: the name of the table to store data into.
    * `db`: the (optional) destination database.

!!! info
    By default the destination is the database defined by `default_db` in `project.yaml`. `db` can be specified to change this, in which case the connection specified needs to:

      * Be a credential from the `required_credentials` list in `project.yaml`.
      * Be defined in your `settings.yaml`.
      * Be one of the supported [databases](../databases/overview.md).

The table specified in `destination` will be affected by prefixes, suffixes and overrides as described in [database objects](../database_objects.md). The source table however will be interpreted literally.

By default, tables will be copied in full every time SAYN runs replacing the table with the newly
pulled data. This behaviour can be altered with the following:

* `incremental_key`: the column to use to determine what data is new. The process will transfer
  any data in the source table with an `incremental_key` value superior or equal to the maximum
  found in the destination, or with a `NULL` value.
* `delete_key`: the column which will be used for deleting data in incremental loads. The process
  will delete any data in the destination table with a `delete_key` value found in the new dataset
  obtained before inserting.
* `append`: a boolean flag indicating if data should be replaced in the destination. This means that
  in full load mode (`incremental_key` not specified) records will be appended rather than the table
  being recreated every time; and in incremental mode records will not be removed, so `delete_key`
  shouldn't be specified. Additionally an extra column `_sayn_load_ts` will be added to the destination
  table to help with de-duplication.

!!! example "tasks/base.yaml"
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

!!! example "tasks/base.yaml"
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
      append: True
    ```
In this other example, whenever the task runs it checks the latest value of `updated_at` and appends to the
destination table every record in the source with an `updated_at` greater than or equal to the maximum value
present in the destination.

While the task is running, SAYN will get records from the source database and load into a temporary table,
and will merge into the destination table once all records have been loaded. The frequency of loading
into this table is determined by the value of `max_batch_rows` as defined in the credentials for the
destination database, which defaults to 50000. However this behaviour can be changed with 2 properties:

* `max_batch_rows`: this allows you to overwrite the value specified in the credential for this task only.
* `max_merge_rows`: this value changes the behaviour so that instead of merging into the destination
  table once all rows have been loaded, instead SAYN will merge after this number of records have been
  loaded and then it will repeat the whole process. The advantage of using this parameter is that for
  copies that take a long time, an error (ie: loosing the connection with the source) wouldn't result
  in the process having to be started again from the beginning.

!!! warning
    When using `max_merge_rows` SAYN will loop through the merge load and merge process until the number
    of records loaded is lower than the value of `max_merge_rows`. In order to avoid infinite loops, the
    process will also stop after a maximum of 100 iteration. To avoid issues, it should be set to a very
    large value (larger than `max_batch_rows`).

## Data types and columns

`copy` tasks accept a `columns` field in the task definition in the same way that `autosql` does. With this
specification, we can override the default behaviour of copy when it comes to column types by enforcing
specific column types in the final table:

!!! example "tasks/base.yaml"
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
      columns:
        - id
        - name: updated_at
          type: timestamp
    ```

In this example we define 2 columns for `task_copy`: `id` and `updated_at`. This will make SAYN:
1. Copy only those 2 columns, disregarding any other columns present at source
2. Infer the type of `id` based on the type of that column at source
3. Enforce the destination table type for `updated_at` to be `TIMESTAMP`

An additional property `dst_name` in columns is also supported. Specifying this property will
change the name of the column in the destination table. When using this property, `delete_key`
and `incremental_key` need to reference this new name.

!!! example "tasks/base.yaml"
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
      incremental_key: updated_ts
      delete_key: id
      columns:
        - id
        - name: updated_at
          dst_name: updated_ts
    ```

In this example, the `updated_at` column at source will be called `updated_ts` on the target.
Note the name in `incremental_key` uses the name on the target.

Additionally, in the `ddl` property we can specify indexes and permissions like in `autosql`.
Note that some databases support specific DDL other than these.
