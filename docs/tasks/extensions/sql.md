# `sql` Task

## About

The `sql` task lets you execute any SQL statement. You can have multiple SQL statements within one file.

## Defining `sql` Tasks In `models.yaml`

A `sql` task is defined as follows:

```yaml
task_sql:
  type: sql
  file_name: sql_task.sql
```

`sql` tasks only have one parameter that needs to be set:

- `file_name`: the name of the file **within the `sql` folder** of the project's root. SAYN automatically looks into this folder so there is no need to prepend `sql/` to the`file_name`.
