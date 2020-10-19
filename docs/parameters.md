# Parameters

`parameters` are a really powerful SAYN feature. They enable you to make your code dynamic and easily switch between profiles. For example, `parameters` are key to separating development and production environments.

SAYN uses [Jinja](https://jinja.palletsprojects.com/){target="\_blank"} templating for both the SQL queries and YAML properties. `parameters` can also be accessed in `python` tasks.

## Project parameters

Project parameters are defined in `project.yaml`:

!!! example "project.yaml"
    ```yaml
    parameters:
      user_prefix: ''
      schema_logs: analytics_logs
      schema_staging: analytics_staging
      schema_models: analytics_models
    ```

The value set in `project.yaml` is the default value those parameters will have. This should match
with the value used on production.

!!! note
    Parameters are interpreted as yaml values, so for example `schema_logs` above would end up
    as a string. In the above example `user_prefix` would also be a string (empty string by default)
    because we included the double quote, but if we didn't include those quotations, the value
    would be python's `None` we we use it in both python and sql tasks.

To override those default values, we just need to set them in the profile. For example, for a dev
environment we can do the following:

!!! example "settings.yaml"
    ```yaml
    # ...
    
    default_profile: dev
    
    profiles:
      dev:
        credentials:
          # ...
        parameters:
          user_prefix: songoku_
          schema_logs: analytics_adhoc
          schema_staging: analytics_adhoc
          schema_models: analytics_adhoc

      prod:
        credentials:
          # ...
    ```

In the above, we're overriding the values of the project parameters for the `dev` profile,
but not for the `prod` profile.

## Task parameters

Tasks can also define parameters. This is useful if there's a way for several tasks to share
the same code:

!!! example "dags/base.yaml"
    ```yaml
    task1:
      type: sql
      file_name: task_template.sql
      parameters:
        src_table: 'table1'

    task2:
      type: sql
      file_name: task_template.sql
      parameters:
        src_table: 'table2'
    ```

!!! example "sql/task_template.yaml"
    ```sql
    SELECT dt
         , COUNT(1) AS c
      FROM {{ src_table }}
     GROUP BY 1
    ```

In the above example both `task1` and `task2` are sql tasks pointing at the same file
`sql/task_template.sql`, the difference between the 2 is the value of the `src_table` parameter
which is used to change the source table in the SQL.

## Using parameters

### Using parameters in `tasks`

Task attributes are interpreted as [Jinja](https://jinja.palletsprojects.com/){target="\_blank"}
parameters. Therefore, you can make the tasks' definition dynamic. This example uses an `autosql`
task:

!!! example "dags/base.yaml"
    ```yaml
    task_autosql_param:
      type: autosql
      file_name: task_autosql_param.sql
      materialisation: table
      destination:
        tmp_schema: '{{ schema_staging }}'
        schema: '{{ schema_models }}'
        table: '{{ user_prefix }}task_autosql_param'
    ```

In this example we're using `schema_staging`, `schema_models` and `user_perfix` project parameters
so that the values would change depending on the profile. Note the use of quotation in the yaml file
when we template task properties.

When running `sayn run -t task_autosql_param`, this would be interpreted based on the `dev` profile,
which we set as default above and evaluate as:

!!! example
    ```yaml
    task_autosql_param:
      type: autosql
      file_name: task_autosql_param.sql
      materialisation: table
      destination:
        tmp_schema: analytics_adhoc
        schema: analytics_adhoc
        table: songoku_task_autosql_param
    ```

If we used the `prod` profile instead (`sayn run -t task_autosql_param -p prod`) the task will evaluate as:

!!! example
    ```yaml
    task_autosql_param:
      type: autosql
      file_name: task_autosql_param.sql
      materialisation: table
      destination:
        tmp_schema: analytics_staging
        schema: analytics_models
        table: task_autosql_param
    ```

This task example even more powerful when used in presets in combination with the jinja variable `task`:

!!! example "dags/base.yaml"
    ```yaml
    presets:
      preset_auto_param:
        type: autosql
        file_name: '{{ task.name }}.sql'
        materialisation: table
        destination:
          tmp_schema: '{{ schema_staging }}'
          schema: '{{ schema_models }}'
          table: '{{ user_prefix }}{{ task.name }}'

    tasks:
      task_autosql_param:
        preset: preset_auto_param
    ```

Here we extract all values from `task_autosql_param` into a preset `preset_auto_param` that can be reused
in multiple tasks. The name of the task is then used to reference the correct sql file and the correct
table name using `{{ task.name }}`

### In SQL queries

For SQL related tasks (`autosql`, `sql`), `parameters` within the SQL code with the same jinja syntax
`{{ parameter_name }}`:

!!! example "sql/task_autosql_param.sql"
    ```sql
    SELECT mt.*
      FROM {{schema_models}}.{{user_prefix}}my_table AS mt
    ```

This SQL query would then be compiled with the relevant `paramaters` based on the profile of the execution.
If using the `dev` profile, this would therefore be compiled as:

!!! example "compiled/base/task_autosql_param.sql"
    ```sql
    SELECT mt.*
      FROM analytics_adhoc.songoku_my_table AS mt
    ```

### In Python Tasks

Parameters are accessible to python tasks as well as properties of the task class with
`self.project_parameters`, `self.task_parameters` and `self.parameters`, which are all python dictionaries.
`self.parameters` is the most convenient one as it combines both project and task parameters in a single
dictionary.

!!! example "python/task_python.py"
    ```python
    from sayn import PythonTask

    class TaskPython(PythonTask):
        def run(self):
            param1 = self.parameters['param1']

            # Some code using param1

            return self.success()
    ```
