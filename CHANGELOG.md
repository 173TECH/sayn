# Change Log

## [0.2.1] - 2020-06-04

### Fixed

- Re-releasing due to issue when uploading to PyPI

## [0.2.0] - 2020-06-03

### Fixed

- Fixed errors when missing temporary schema in autosql tasks
- Fixed autocommit issues with snowflake
- Fixed crashing bug when parsing credentials

### Changed

- Renames the following:
  - models.yaml > project.yaml
  - groups > presets
  - models > dags
  - to > destination (copy and autosql task types)
  - from > source (copy task types)
  - staging_schema > tmp_schema (copy and autosql task types)
- Alows specifying presets in project.yaml
- Presets in the dags can reference a preset in project.yaml with the preset property
- Compilation output is saved in a folder named as the dag within the compile folder
- module in python tasks is deprecated and the class should now point at the full class path (ie: class: my_module.MyTask points at a sayn python task called MyTask within python/my_module.py
- Task and preset names are restricted by a regex ^[a-zA-Z0-9][-_a-zA-Z0-9]+$
