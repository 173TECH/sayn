# Change Log

## [0.4.0] - 2020-10-19

### Changed

- Switched to pydantic for project validation
- Removed Config and Dag objects and split that functionality into separate modules improving the ability for automated testing
- Created an App object to encapsulate most of the the running logic
- Created a TaskWrapper object to isolate the task lifetime logic from the execution
- Added new Result type for error reporting
- Switched from standard python logging to an event reporting model
- Major changes to console UI
- Added the concept of task step to improve feedback to the user
- With `-d` sayn will write all sql related to every step in the compile folder

## [0.3.0] - 2020-07-29

### Changed

- Allows indexes definition without column definition under ddl for autosql
- Reworks the db credentials specifications
- Adds Redshift distribution and sort table attributes
- Adds Redshift connection through IAM temporary passwords
- Added MySQL
- Updated copy task to latest changes of db drivers
- Added select_stream to improve performance in copy tasks
- Added load_data_strem to postgresql for bulk loading in copy tasks
- Changed underlying structure of logging
- Added first tests

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
