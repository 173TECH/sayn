# DAGs

Tasks in a SAYN project define a DAG (Direct Acyclic Graph). Dependencies between tasks are defined with
the `parents` list. For example, the SAYN tutorial defines the following DAG:

![Tutorial](dag.png)

## Building our DAG

When a SAYN project contains many tasks, we can split the definition into multiple files. These files
are yaml files stored in the `dag` folder. To include these in the project, we do that by specifying them
in the `dags` list in `project.yaml` (note we don't specify `.yaml`):

!!! example "project.yaml"
    ```yaml
    dags:
      - base
      - marketing
      - data_science
    ```

Then in each dag file we can define tasks (required) and presets (optional).

!!! example "dags/base.yaml"
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
| tasks | The set of tasks that compose the DAG. For more details on `tasks`, please see the [Tasks](tasks/overview.md) section. | Yes |
| presets | Defines preset task structures shared by several tasks. Presets defined within DAG files can inherit from presets defined at the project level in `project.yaml`. See the [Presets](presets.md) section for more details. | Optional |
