# Change Log

## Next release

### Changed

- Refactoring of database code to improve performance by adding introspection
  after all tasks have been setup
- Improvements to BigQuery process
- Adds support for testing all databases
- Adds support for environment variables specified in YAML (JSON still supported as well)

## [0.5.4] - 2021-01-11

### Fixed

- Duplication of data in sample project

## [0.5.3] - 2021-01-05

### Fixed

- Fixes unicode issues with load_date in bigquery

## [0.5.2] - 2020-12-16

### Changed

- Adds BigQuery support

## [0.5.1] - 2020-12-11

### Changed

- Renamed task attribute task_group to group

## [0.5.0] - 2020-12-09

### Changed

- Changed concept of dags to tasks
- Added automated detection of task files in tasks folder
- Added option to change database destination

### Fixed

- Fixed issues with primary key DDLs

## [0.4.2] - 2020-11-11

### Changed

- Fixed bug preventing SQL task execution introduced in 0.4.1

## [0.4.1] - 2020-11-03

### Changed

- Database methods raise exceptions rather than return Result objects
- load_data automatically creates tables
- max_batch_rows introduced to allow manipulating the size of batches in load_data

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
