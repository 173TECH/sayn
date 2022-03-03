# `autosql` Task

## About

The `autosql` task lets you write a `SELECT` statement and SAYN then automates the data processing (i.e. table or view creation, incremental load, etc.) for you.

## Defining `autosql` Tasks

An `autosql` task group is defined as follows:

!!! example "project.yaml"
    ```
    ...

    groups:
      core:
        type: autosql
        file_name: "core/*.sql"
        materialisation: table
        destination:
          table: "{{ task.name }}"

    ...
    ```

An `autosql` task is defined by the following attributes:

* `type`: `autosql`.
* `file_name`: the path to a file **within the sql folder of the project's root**. When defining `autosql` groups in `project.yaml` this property needs to be a glob expression, for example `group/*.sql`.
* `materialisation`: this should be either `table`, `view` or `incremental`. `table` will create a table, `view` will create a view. `incremental` will create a table and will load the data incrementally based on a delete key (see more detail on `incremental` below).
* `destination`: this sets the details of the data processing.
    * `tmp_schema`: the (optional) schema which will be used to store any necessary temporary object created in the process. The final compiled value is affected by `schema_prefix`, `schema_suffix` and `schema_override` as specified in [database objects](../database_objects.md).
    * `schema`: the (optional) destination schema where the object will be created. The final compiled value is affected by `schema_prefix`, `schema_suffix` and `schema_override` as specified in [database objects](../database_objects.md).
    * `table`: is the name of the object that will be created. The final compiled value is affected by `table_prefix`, `table_suffix` and `table_override` as specified in [database objects](../database_objects.md).
    * `db`: the (optional) destination database.
* `delete_key`: specifies the incremental process delete key. This is for `incremental` `materialisation` only.

!!! info
    By default the task is executed in the database defined by `default_db` in `project.yaml`. `db` can be specified to change this, in which case the connection specified needs to:

      * Be a credential from the `required_credentials` list in `project.yaml`.
      * Be defined in your `settings.yaml`.
      * Be one of the supported [databases](../databases/overview.md).

## Setting Dependencies With `autosql`

With `autosql` tasks, you should use the `src` macro in your `SELECT` statements to implicitly create task dependencies.

!!! example `autosql` query
  ```
  SELECT field1
       , field2
    FROM {{ src('my_table') }} l
  ```

By using the `{{ src('my_table') }}` in your `FROM` clause, you are effectively telling SAYN that your task depends on the `my_table` table (or view). As a result, SAYN will look for the task that produces `my_table` and set it as a parent of this `autosql` task automatically.

!!! tip
    When using the `src` macro, you can pass a structure formatted as `schema.table` such as `{{ src('my_schema.my_table') }}`. In this case, SAYN interprets the first element as the schema, the second element as the table or view. If you use `schema_prefix` and / or `table_prefix` in your project settings, SAYN will then prepend the `schema_prefix` to the `schema` value and `table_prefix` to the `table` value. For example, if your `schema_prefix` is set to `analytics` and `table_prefix` to `up` then `{{ src('my_schema.my_table') }}` will compile `analytics_my_schema.up_my_table`.

## Advanced Configuration

If you need to amend the configuration (e.g. materialisation) of a specific `autosql` task within a `group`, you can overload the values specified in the YAML group definition. To do this, we simply call `config` from a Jinja tag within the sql file of the task:

!!! example "autosql with config"
    ```
    {{ config(materialisation='view') }}

    SELECT ...
    ```

The above code will override the value of `materialisation` setting defined in YAML to make this model a view. All other parameters
described above in this page are also available to overload with `config` except `db`, `file_name` and `name`.

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

## Defining columns

Autosql tasks accept a `columns` field in the task definition that affects the table creation by enforcing types and column order.

!!! attention
      Each supported database might have specific DDL related to it. Below are the DDLs that SAYN supports across all databases. For DDLs related to specific databases see the database-specific pages.

### CREATE TABLE DDLs

SAYN also lets you control the CREATE TABLE statement if you need more specification. This is done with:

* columns: the list of columns including their definitions.
* table_properties: database specific properties that affect table creation (indexes, cluster, sorting, etc.).
* post_hook: SQL statments executed right after the table/view creation.

`columns` can define the following attributes:

* name: the column name.
* type: the column type.
* tests: list of keywords that constraint a specific column
  - unique: enforces a unique constraint on the column.
  - not_null: enforces a non null constraint on the column.
  - allowed_values: list allowed values for the column.

`table_properties` can define the following attributes (database specific):
* indexes:
* sorting: specify the sorting for the table
* distribution_key: specify the type of distribution.
* partitioning: specify the partitioning model for the table.
* clustering: specify the clustering for the table.

!!! attention
      Each supported database might have specific `table_properties` related to it; see the database-specific pages for further details and examples.

!!! Attention
    If the a primary key is defined in both the `columns` and `indexes` DDL entries, the primary key will be set as part of the `CREATE TABLE` statement only.

!!! example "autosql with columns"
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
        columns:
          - name: x
            type: int
            primary: True
          - name: y
            type: varchar
            unique: True
        permissions:
          role_name: SELECT
    ...
    ```
