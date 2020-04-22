This is an example SAYN project. It shows you how to implement and use SAYN for data modelling and processing.

For more details, you can see the documentation here: link.

----
Quick overview:

SAYN uses 2 key files to control the project:
  - settings.yaml: individual settings which are not shared
  - models.yaml: project settings and tasks definitions

SAYN code is stored in 3 main folders:
  - models: for additional models if necessary
  - sql: for SQL tasks
  - python: for python tasks

SAYN uses some key commands for run:
  - sayn run: run the whole project
    - -p flag to specify a profile when running sayn: e.g. sayn run -p prod
    - -t flag to specify tasks to run: e.g. sayn run -t task_name
    - -m flag to specify a model to run: e.g. sayn run -m model_name
  - sayn compile: compiles the code (similar flags apply)
  - sayn --help for full detail on commands
