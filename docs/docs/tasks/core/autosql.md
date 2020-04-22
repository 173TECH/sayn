# `autosql` Task

## About

The `autosql` task lets you write a `SELECT` statement and SAYN then automates the data processing (i.e. table or view creation, incremental load, etc.) for you.

## Defining `autosql` Tasks In `models.yaml`

An `autosql` task is defined as follows:

```yaml
task_autosql:
  file_name: task_autosql.sql
  type: autosql
  materialisation: table
  to:
    staging_schema: analytics_staging
    schema: analytics_models
    table: task_autosql
```

It has a task name (`task_autosql` here), and then defines the following mandatory attributes:

- `file_name`: the name of the file **within the sql folder of the project's root**.
- `type`: the task type, this needs to be one the the task types supported by SAYN.
- `materialisation`: this should be either `table`, `view` or `incremental`. `table` will create a table, `view` will create a view. `incremental` will create a table and will load the data incrementally based on a delete key (see more detail on `incremental` below).
- `to`: this sets the details of the data processing. `staging_schema` specifies the schema which will be used to store any necessary temporary object created in the process. `schema` is the destination schema where the object will be created. `table` is the name of the object that will be created.

*Note: `staging_schema` and `schema` are actually optional. Not specifying those will use the database connection's default schema. We do recommend to set those however to prevent mistakes.*

In addition, `autosql` has the following optional attribute:

- `ddl`: can be used to control the DDLs used during the process execution. `ddl` is a mapping.

Here are the lists of available `ddl` options:

- `permissions`: automatically grants permissions on the created object to specified roles and users.
- `primary_key`: sets the primary key on the table.
- `indexes`: sets an index on the table.
- `columns`: enables to specify the column types.
- `delete_key`: specifies the incremental process delete key. This is for `incremental` `materialisation` only.

Please see below an example with all `ddl` parameters set for an `autosql` task materialising into a `table`:

```yaml
task_autosql_ddl:
  file_name: test_sql_incremental.sql
  group: modelling_incremental
  ddl:
    primary_key:
      - listing_id
    columns:
      listing_id:
        type: integer
      listing_name:
        type: varchar
    indexes:
      - listing_id
```

## Using `autosql` In `incremental` Mode

If you do not want to have a full refresh of your tables, you can use the `autosql` task with `incremental` `materialisation`. This is extremely useful for large data volumes when full refresh would be too long.

SAYN `autosql` tasks with `incremental` materialisation require at least a `delete_key` (a list of fields) to be set as part of the `ddl` attributes. Please see below an example:

```yaml
task_autosql_incremental:
  file_name: task_autosql_incremental.sql
  type: autosql
  materialisation: incremental
  to:
    staging_schema: analytics_staging
    schema: analytics_models
    table: task_autosql
  ddl:
    delete_key:
      - dt
```

When using `incremental`, SAYN will do the following in the background:

1. Create a temporary table based on the incremental logic from  the SAYN query.
2. Delete any record in the destination table which has a value equal to any value that can be found in the temporary table on the delete key.
3. Load the temporary table in the destination table.
