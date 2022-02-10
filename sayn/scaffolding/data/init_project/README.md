This is an example SAYN project. It shows you how to implement and use SAYN for data processing and modelling.

For more details, you can see the documentation here: https://173tech.github.io/sayn/

----
Quick overview:

SAYN uses 2 key files to control the project:
  - settings.yaml: individual settings which are not shared
  - project.yaml: project configuration which are shared across all collaborators on the project.

SAYN code is stored in 2 main folders:
  - sql: code for SQL tasks
  - python: code for python tasks

SAYN uses some key commands for run:
  - sayn run: run the whole project
    - -p flag to specify a profile when running sayn: e.g. sayn run -p prod
    - -t flag to specify tasks to run: e.g. sayn run -t task_name
    - -t group:group_name to specify a task group to run: e.g. sayn run -t group:group_name
  - sayn compile: compiles the code (similar flags apply)
  - sayn --help for full detail on commands
