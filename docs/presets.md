# Presets

Presets are used to define common task configuration. If a `task` specifies a `preset` attribute, it
will then inherit all attributes from the referred `preset`. This makes `presets` great to avoid
repetition.

## Defining a `preset`

!!! example "Preset"
    ```yaml
    presets:
      modelling:
        type: autosql
        materialisation: table
        destination:
          tmp_schema: analytics_staging
          schema: analytics_models
          table: '{{task.name}}'
    ```

The above defines a preset called `modelling`. Every `task` referring to it will be an `autosql`
task and inherit all other attributes from it. For a task to use this configuration, we use the `preset`
property in the task.

!!! example "dags/base.yaml"
    ```yaml
    tasks:
      task_name:
        preset: modelling
        #other task properties
    ```

Presets can be defined both in `project.yaml` and in any dag file.

## Preset inheritance

Presets can reference other presets, the behaviour of this reference being exactly as it works for task.

!!! example "project.yaml"
    ```yaml
    presets:
      modelling:
        type: autosql
        materialisation: table
        destination:
          tmp_schema: analytics_staging
          schema: analytics_models
          table: '{{task.name}}'

      modelling_view:
        preset: modelling
        materialisation: view
    ```

In the above example, `modelling_view` is a preset with exactly the same properties as preset `modelling`
except it will generate a view when materialising an autosql task.
