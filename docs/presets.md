# Presets

## About

`presets` enable to define preset tasks which can be used when defining `tasks` in DAGs. If a `task` specifies a `preset` attribute, it will then inherit all attributes from the referred `preset`. This makes `presets` great to avoid repetition.

## Defining a `preset`

`presets` are defined in a similar way than `tasks` within each individual DAG file. Please see below an example of a preset definition:

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

The above defines a `modelling` preset. Every `task` referring to it will be an `autosql` task and inherit all other attributes from the `modelling` preset.

## Defining `tasks` using `presets`

`tasks` can inherit attributes directly from `presets`. In order to do so, specify the `preset` attribute on the task. The below example illustrates this by setting the `modelling` preset defined above on a `task`:

```yaml
tasks:
  #...

  task_preset:
    preset: modelling
    #other task properties

  #...
```

## Defining project-level presets

If you use the same `preset` across multiple DAGs, you can avoid this repetition by defining a project-level `preset`. For example, if you use the `modelling` preset defined above across all DAGs of your SAYN project, you can define it directly in `project.yaml` in a similar way:

**`project.yaml`**
```yaml
# ...

presets:
  modelling:
    type: autosql
    materialisation: table
    destination:
      tmp_schema: analytics_staging
      schema: analytics_models
      table: '{{task.name}}'

# ...
```

You can then use that project-level `preset` to define `presets` within individual DAGs as follows:

**`dag.yaml`**
```yaml
presets:
  modelling:
    preset: modelling

# ...
```
