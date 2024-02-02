# Change Log

## [0.6.13] - 2024-02-02

### Fixed

- Avoid double execution of tests on run command
- Don't execute tests when task failed

## [0.6.12] - 2024-01-19

### Changed

- Snowflake: temporary objects are created as `temp` where possible
  (ie: stages, incremental tables,...)
- Added flag to run command (--with-tests) that combines the execution of
  both tests and tasks
- Added new flag to run and test commands (--fail-fast) that terminates the
  execution upon an error on any task or test

## [0.6.11] - 2023-08-30

### Fixed

- Allow changing the target database for tests
- Fixes load_data code when checking if the table exists
- Fixes issue with Redshift region setting for IAM connections

### Changed

- Update driver versions for Redshift, Snowflake and BigQuery

## [0.6.10] - 2023-05-18

### Fixed

- Issue with incremental sql models on first load not creating the table

## [0.6.9] - 2023-04-05

### Fixed

- Colon (:) in SQL is not interpreted as bind parameters
- Update Redshift dependencies
- Fixes to Redshift load_data when using an S3 bucket

## [0.6.8] - 2023-04-03

### Changed

- Add support for 3rd level (eg: projects in BigQuery or databases in Snowflake)
  when referencing database objects with `src` and `out`
- Add support for python 3.11
- Switch Redshift driver to use AWS' redshift-connector
- Improve support for Redshift IAM authentications

### Fixed

- Allow data types other than strings in `allowed_values` data test
- Fixes to ddl parsing for view materialisations

## [0.6.7] - 2022-09-20

### Changed

- Drop support for Python 3.7
- Upgrades numpy version
- Improved messaging upon load_data errors
- Removed Jinja caching to allow more code reusability

### Fixed

- CLI command status code changed to error when tasks fail
- Fixed error in src or out to allow for missing schema

## [0.6.6] - 2022-09-03

### Fixed

- Fixes serialisation to json preventing copy tasks to work on BigQuery
- Fixes character encoding issue on windows

## [0.6.5] - 2022-07-19

### Changed

- sql tasks replace autosql with new materialisation "script"
- Allows custom tests to be defined from project.yaml as a group with the type: test
- Root for custom tests changed to the sql folder, rather than sql/tests

### Fixed

- UUID support for copy to BigQuery
- Adds support for missing task properties to config macro and python task decorator
- Fixes to introspection with missing connections
- Upstream prod and from_prod are recognised with sayn test


## [0.6.4] - 2022-04-06

### Changed

- Allows connections for decorator based python tasks to be missing from settings
  when the task is not in the current execution
- Allows adding tags from decorator based python tasks
- Adds src and out to jinja environment in python tasks


## [0.6.3] - 2022-03-02

### Changed

- Supports `src` in custom tests

### Fixed

- Issue with copy tasks from the `default_db`
- Test results completion improvements


## [0.6.2] - 2022-02-24

### Changed

- New settings `from_prod` allows to mark tables that are always read from production
- New settings `default_run` allows to define a default run filter
- Apply db object transformations only to `defaul_db`
- Allow columns in copy to be specified with just the name
- Renamed `values` to `allowed_values` in data testing
- Various improvements to cli messaging
- Refactoring of db objects internal code
- Allows SAYN to run when unused credentials are missing from settings

### Fixed

- Pin dependency version to resolve issue with MarkupSafe

## [0.6.1] - 2022-01-25

### Fixed

- Fixes to bigquery introspection

## [0.6.0] - 2022-01-24

### New

- Task groups can be generated from automatically from a path specification
- Task dependencies can be set in code using src and out macros without YAML
- Simpler pattern for creating python tasks based on decorators

### Changed

- BigQuery driver upgraded to sqlalchemy-bigquery 1.3.0, deprecating pybigquery

### Fixed

- Fixes to incremental autosql tasks in BigQuery
- Fixes to credentials_path property for BigQuery credentials

## [0.5.13] - 2021-12-13

### Changed

- Enables installation on Python 3.10

### Fixed

- Fixes colour of last message in SAYN cli

## [0.5.12] - 2021-11-24

### Fixed

- Allow max_merge_rows with append copy tasks

## [0.5.11] - 2021-11-23

### Changed

- Adds append only mode for copy tasks

### Fixed

- Make parameter and credential names case lowercase allowing environment variables on Windows
- Bigquery support for changing autosql models between views and tables
- Better error messages when additional properties are specified in a task definition

## [0.5.10] - 2021-09-06

### Fixed

- Fixes issue with NaN values when loading to Snowflake

## [0.5.9] - 2021-09-02

### Fixed

- Fixes issue with filters in the cli

## [0.5.8] - 2021-09-01

### Changed

- Adds on_fail functionaly to tasks allowing children to run when parent tasks fail
- Improvements to cli to allow lists with a single -t or -x flag

## [0.5.7] - 2021-06-25

### Changed

- Adds support for renaming columns on copy tasks
- Improvements to BigQuery data load of nested fields

## [0.5.6] - 2021-04-29

### Changed

- Adds staging area based batch copy for snowflake
- Returns number of records loaded in load_data
- Adds support for copy to merge frequently to target table

### Fixed

- Fixes to columns without names in ddl
- Adds sorting to copy's get_data_query

## [0.5.5] - 2021-03-18

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
