# Models: `models.yaml`

## Role

`models.yaml` is the backbone of a SAYN project where tasks and their parentage are defined. This is what SAYN will use to create and run the DAG. This file is shared across all users of the SAYN project.

## Content

**`models.yaml`**

``` yaml
sayn_project_name: [company]_sayn_etl

default_db: warehouse

required_credentials:
  - warehouse

parameters:
  table_prefix: ''
  schema_logs: analytics_logs
  schema_staging: analytics_staging
  schema_models: analytics_models

groups:
  modelling:
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'

tasks:
  task_1:
    file_name: task_1.sql
    type: sql

  task_2:
    file_name: task_2.sql
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'

  task_3:
    file_name: task_3.sql
    group: modelling

  task_4:
    file_name: task_4.sql
    group: modelling
    materialisation: view
    parents:
      - task_3

  task_5:
    module: task_5
    type: python
    class: TaskFive
    parents:
      - task_1
      - task_4
```

The `models.yaml` has the following mandatory parameters defined:

- `sayn_project_name`: the name of your SAYN project.
- `default_db`: this is the database that will be used by tasks writing to the analytics warehouse (e.g. `sql`, `autosql` tasks). The `default_db` that is set should be part of the `required_credentials`.
- `required_credentials`: the list of database and API credentials which are required to run the SAYN project. The credential details are then specified in `settings.yaml`.
- `tasks`: the list of tasks and their definitions. Those will be used by SAYN to create and run the DAG.

The `models.yaml` has the following optional parameters defined:

- `parameters`: parameters are used to customise tasks. SQL queries are effectively Jinja templates so those parameters can be used in queries. They can also be accessed in Python tasks via the task object attributes. `parameters` can be overridden with `settings.yaml` which means that user can set specific `parameters` for testing or separate development environments.
- `groups`: groups enable to define some attributes which will be used by all tasks referring to the group.
- `models`: models enable to add additional models to your DAG. Those additional model files should be YAML files stored in a `models` folder at the root level of the project. For example, all marketing tasks could be in `models/marketing.yaml`.
