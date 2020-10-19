# Settings: `project.yaml`

The `project.yaml` defines the core components of the SAYN project. It is **shared across all collaborators**.

!!! example "project.yaml"
    ``` yaml
    required_credentials:
      - warehouse
    
    default_db: warehouse

    dags:
      - base
    
    parameters:
      user_prefix: ''
      schema_logs: analytics_logs
      schema_staging: analytics_staging
      schema_models: analytics_models

    presets:
      preset1:
        type: sql
        file_name: '{{ task.name }}.sql'
    ```

| Property | Description | Default |
| -------- | ----------- | -------- |
| required_credentials | The list of credentials used by the project. Credentials details are defined the `settings.yaml` file. | Required |
| default_db | The credential used by default by sql and autosql tasks. | Entry in `required_credentials` if only 1 defined |
| dags | List of files from the dag folder that contains all tasks definitions. | Required |
| parameters | Project parameters used to make the tasks dynamic. They are overwritten by `profile` `parameters` in `settings.yaml`. See the [Parameters](../parameters.md) section for more details. | |
| presets | Defines preset task structures so task can inherit attributes from those `presets` directly. See the [Presets](../presets.md) section for more details. | |
