# Parameters

## About

`parameters` are a really powerful SAYN feature. They enable you to make your code dynamic and easily switch between profiles. For example, `parameters` are key to separating development and production environments.

SAYN uses [Jinja](https://jinja.palletsprojects.com/){target="\_blank"} templating for both the SQL queries and some YAML files. `parameters` can also be accessed in `python` tasks.

## Defining Parameters

`parameters` are defined at several levels:

* `project.yaml` file: defines the project's default parameters and their values.
* `settings.yaml` file: defines profiles and their parameters. The parameter values of a profile will override the project's default parameters from `project.yaml`.
* `presets`: a preset can set a `parameters` attribute so all tasks within that preset inherit those.
* `tasks`: a task can set a `parameters` attribute.

## Accessing Parameters

For the below section, we consider a project with the following setup:

**project.yaml**

```yaml
# ...

parameters:
  user_prefix: ''
  schema_logs: analytics_logs
  schema_staging: analytics_staging
  schema_models: analytics_models

# ...
```

**settings.yaml**

```yaml
# ...

default_profile: dev

profiles:
  dev:
    # ...
    parameters:
      user_prefix: songoku_
      schema_logs: analytics_adhoc
      schema_staging: analytics_adhoc
      schema_models: analytics_adhoc
  prod:
    # ...
    parameters:
      user_prefix: ''
      schema_logs: analytics_logs
      schema_staging: analytics_staging
      schema_models: analytics_models
```

### In `tasks`

Task attributes are interpreted as [Jinja](https://jinja.palletsprojects.com/){target="\_blank"} parameters. Therefore, you can make the tasks' definition dynamic. This example uses an `autosql` task:

```yaml
task_autosql_param:
  file_name: task_autosql_param.sql
  type: autosql
  materialisation: table
  destination:
    tmp_schema: '{{schema_staging}}'
    schema: '{{schema_models}}'
    table: '{{user_prefix}}{{task.name}}'
```

*Note: the `table` setting uses `{{task.name}}`. This is because the task object is in the Jinja environment and you can therefore access any task attribute. In this case, `{{task.name}}` is `task_autosql_param`.*

When running `sayn run -t task_autosql_param`, this would be interpreted as (SAYN uses the `default_profile` by default):

```yaml
task_autosql_param:
  file_name: task_autosql_param.sql
  type: autosql
  materialisation: table
  destination:
    tmp_schema: analytics_adhoc
    schema: analytics_adhoc
    table: songoku_task_autosql_param
```

If the user desires to run with production parameters, this can be done by leveraging the profile flag: `sayn run -t task_autosql_param -p prod`. This would therefore use the `prod` profile parameters and interpret the above block as follows:

```yaml
task_autosql_param:
  file_name: task_autosql_param.sql
  type: autosql
  materialisation: table
  destination:
    tmp_schema: analytics_staging
    schema: analytics_models
    table: task_autosql_param
```

### In `presets`

`parameters` can be accessed in `presets` in the same way than they are accessed in `tasks`. For example, you could have the following `modelling` preset definition:

```yaml
presets:
  modelling:
    type: autosql
    materialisation: table
    destination:
      tmp_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{user_prefix}}{{task.name}}'
```

The interpretation of this preset will work as in the above section, using `parameters` from the relevant profile at execution time.

### In SQL Queries

For SQL related tasks (`autosql`, `sql`), `parameters` can be accessed in SQL queries with the following syntax: `{{parameter_name}}`. For example, `task_autosql_param` defined above could refer to the following query:

**sql/task_autosql_param.sql**

```sql
SELECT mt.*

FROM {{schema_models}}.{{user_prefix}}my_table AS mt
```

This SQL query would then be compiled with the relevant `paramaters` based on the profile of the execution. If using the `dev` profile, this would therefore be compiled as:

```sql
SELECT mt.*

FROM analytics_adhoc.songoku_my_table AS mt
```

### In Python Tasks

`parameters` can be accessed in `python` tasks via the SAYN API as they are stored on the Task object:

* `self.sayn_config.parameters`: accesses the project `parameters` set in `project.yaml` and `settings.yaml`.
* `self.parameters`: accesses the task's `parameters`.

For example, you could have the following `python` task code to access your project parameters:

```python
from sayn import PythonTask

class TaskPython(PythonTask):
    def setup(self):
        #code doing setup
        err = False
        if err:
            return self.failed()
        else:
            return self.ready()

    def run(self):
        err = False

        sayn_params = self.sayn_config.parameters
        #code you want to run

        if err:
          return self.failed()
        else:
          return self.success()
```
