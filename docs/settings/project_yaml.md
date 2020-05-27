# Settings: `project.yaml`

## Role

The `project.yaml` defines the core components of the SAYN project. It is **shared across all collaborators**.

## Content

Please see below and example of `project.yaml` file:

**`project.yaml`**
``` yaml
default_db: warehouse

required_credentials:
  - warehouse

dags:
  - base

parameters:
  user_prefix: ''
  schema_logs: analytics_logs
  schema_staging: analytics_staging
  schema_models: analytics_models
```

The `project.yaml` file requires the following to be defined:

* `default_db`: the database use at run time.
* `required_credentials`: the required credentials to run the project. Credentials details are defined the `settings.yaml` file.
* `dags`: the DAGs of the project (this example only imports one DAG). Those DAGs contain the tasks.

In addition, the `project.yaml` file can define the following in order to make the SAYN project more dynamic and efficient:

* `parameters`: those parameters are used to make the tasks dynamic. They are overwritten by `parameters` in `settings.yaml`. See the [Parameters](parameters.md) section for more details.
* `presets`: defines preset task structures so task can inherit attributes from those `presets` directly. See the [Presets](presets.md) section for more details.
