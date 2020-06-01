# DAGs

## About

DAGs (Directed Acyclic Graph) are the essential processes being run by SAYN. They are defined by `yaml` files stored in the `dags` folder.

## Importing a DAG

For a DAG to be able to run, it needs to be imported into the `dags` section of the `project.yaml` file as follows:

**`project.yaml`**
```yaml
#...

dags:
  - base

#...
```

Please note that you should not add the `.yaml` extension when importing a DAG within the `project.yaml` file.

## Creating a DAG

Please see below an example DAG file:

**`dag.yaml`**
```yaml
presets:
  modelling:
    type: autosql
    materialisation: table
    destination:
      tmp_schema: analytics_staging
      schema: analytics_models
      table: '{{task.name}}'

tasks:
  load_data:
    type: python
    class: load_data.LoadData

  #task defined without preset
  dim_tournaments:
    type: autosql
    file_name: dim_tournaments.sql
    materialisation: table
    destination:
      tmp_schema: analytics_staging
      schema: analytics_models
      table: dim_tournaments
    parents:
      - load_data

  #task defined using a preset
  dim_arenas:
    preset: modelling
    file_name: dim_arenas.sql
    parents:
      - load_data

  # ...
```

Each DAG file requires the following to be defined:

* `tasks`: the set of tasks that compose the DAG. For more details on `tasks`, please see the [Tasks](tasks/overview.md) section.

In addition, DAG files can define the following:

* `presets`: defines preset task structures so task can inherit attributes from those `presets` directly. `presets` defined within DAG files can inherit from `preset` defined at the project level in `project.yaml`. See the [Presets](presets.md) section for more details.
