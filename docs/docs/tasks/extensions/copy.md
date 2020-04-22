# `copy` Task

## About

The `copy` task automatically copies data from one database to another. It can be used to automatically ingest data from operational databases (e.g. PostgreSQL, MySQL, etc.) to your analytics warehouse.

## Defining `copy` Tasks In `models.yaml`

A `copy` task is defined as follows:

```yaml
task_copy:
  type: copy
  from:
    db: from_db
    schema: from_schema
    table: from_table
  to:
    staging_schema: staging_schema
    schema: schema
    table: table_name
  ddl:
    columns:
      - name: column
  incremental_key: column
  delete_key: column
```

`copy` tasks have the following parameters that need to be set:

* `from`: the source details
    * `db`: the source database, this should be part of the `required_credentials` in `models.yaml`
    * `schema`: the source schema.
    * `table`: the source table.
* `to`: the destination details. The destination database is the `default_db` set in `models.yaml`.
    * `staging_schema`: the staging schema used in the process of copying data.
    * `schema`: the destination schema.
    * `table`: the destination schema.
* `ddl`: setting the DDL of the process
    * `columns`: a list of columns to export

The following parameters are optional:

* `incremental_key`: the column which will be used for incremental loads. The process will transfer any data with an `incremental_key` value superior to the maximum found in the source table.
* `delete_key`: the column which will be used for deleting data in incremental loads. The process will delete any data in the destination table with a `delete_key` value superior to the maximum found in the source table.
