# Groups

## About

`groups` are defined in `models.yaml` and enable to define some shared parameters across tasks. If a task in `models.yaml` refers to a group it will inherit all the properties of the group. `groups` are a great way to avoid repetition.

## Usage

A task refers to a `group` with the `group` attribute, such as follows:

```yaml
task_group:
  #other task properties
  group: my_group
```

For example, for our data modelling, we could define a `modelling` group as follows:

**models.yaml**
```yaml
#your models settings
groups:
  modelling:
    type: autosql
    materialisation: table
    to:
      staging_schema: analytics_staging
      schema: analytics_models
      table: {{task.name}}

tasks:
  #some tasks

  model_1:
    file_name: model_1.sql
    group: modelling

  model_2:
    file_name: model_2.sql
    group: modelling

  #more tasks belonging to the modelling group
```

This will imply that the tasks `model_1` and `model_2` will be `autosql` tasks, materialise as tables and share all the other attributes of the `modelling` group.

*Note: in the `modelling` group, the `table` parameter is set as `{{task.name}}`. This is a dynamic parameter which is discussed in the [Parameters](parameters.md) section.* 
