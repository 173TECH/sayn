# `python` Task

## About

The `python` task lets you use Python for your task. It will run a Python Class which you define. Therefore, those tasks can do anything Python can do. They are extremely useful for data extraction or data science models.

## Defining `python` Tasks In `models.yaml`

A `python` task is defined as follows:

```yaml
task_python:
  type: python
  class: task_python.TaskPython
```

It is defined by the following attributes:

- `type`:`python`.
- `class`: the import statement that should be executed to import the Python Class. Please note that Python tasks scripts should be saved in the `python` folder within the project's root.

Please note that for the `python` tasks to run, you must have an `__init__.py` file into the `python` folder so it is treated as a package.

## Writing A `python` Task

### Basics

Please see below an example of a `python` task:

``` python
from sayn import PythonTask

class TaskPython(PythonTask):
    def setup(self):
        #code doing setup
        err = False
        if err:
            return self.failed()
        else:
            return self.ready()

    def run(self):
        err = False

        #code you want to run

        if err:
          return self.failed()
        else:
          return self.success()
```

As you can observe, writing a `python` task requires the following:

* import the `PythonTask` class from `sayn`.
* define the class you want the task to run. The task should inherit from `PythonTask`.
* the class you define can overwrite the following methods (those methods need to follow a specific signature and **you should only pass `self` as argument**):
    * `setup`: runs when setting up the task. It should return `self.failed()` if an error has occurred otherwise `self.ready()`.
    * `compile`: runs when compiling the task. It should return `self.failed()` if an error has occurred otherwise `self.success()`.
    * `run`: runs when executing the task. It should return `self.failed()` if an error has occurred otherwise `self.success()`.

### Using the SAYN API

In order, to make your `python` tasks dynamic based on project settings and profiles, you can use the SAYN API:

- `self.sayn_config.parameters`: accesses project config parameters (`project.yaml`, `settings.yaml`). For more details on `parameters`, see the [Parameters](../parameters.md) section.
- `self.sayn_config.api_credentials`: dictionary containing the API credentials available for the profile used at run time.
- `self.sayn_config.dbs`: dictionary containing database objects specified in the profile used at run time.
- `self.parameters`: accesses the task's parameters.
- `self.default_db`: accesses the `default_db` specified in the `project.yaml` file.

Using those parameters is extremely useful in order to tailor your `python` tasks' code in order to separate between development and production environments.

### Logging for Python tasks with the SAYN API

In order to log for Python tasks, you should use the SAYN API. It can be accessed through the `self.logger` attribute on the task. It has the following methods:

* `self.logger.print(self, text)`: print to console.
* `self.logger.debug(self, text)`: debug log to console and file.
* `self.logger.info(self, text)`: info log to console and file.
* `self.logger.warning(self, text)`: warning log to console and file.
* `self.logger.error(self, text)`: error log to console and file.
