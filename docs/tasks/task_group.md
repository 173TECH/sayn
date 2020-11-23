# Task Groups

Task groups are a convenient way to segment and organise your data processes in your SAYN project. Each YAML file in the `tasks` folder represents a task group. By default, SAYN includes any file in the `tasks` folder ending with a `.yaml` extension when creating the DAG (Directed Acyclic Graph).

## Task Groups Structure

Each task group file defines tasks (required) and presets (optional).

!!! example "tasks/base.yaml"
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
    ```

| Property | Description | Required |
| -------- | ----------- | -------- |
| tasks | The set of tasks that compose the task group. For more details on `tasks`, please see the [Tasks](overview.md) section. | Yes |
| presets | Defines preset task structures shared by several tasks. Presets defined within task group files can inherit from presets defined at the project level in `project.yaml`. See the [Presets](../presets.md) section for more details. | Optional |
