# `python` Task

## About

The `python` task lets you use Python for your task. It will run a Python Class which you define. Therefore, those tasks can do anything Python can do. They are extremely useful for data extraction or data science models.

## Defining `python` Tasks In `models.yaml`

A `python` task is defined as follows:

```yaml
task_python:
  type: python
  module: task_python
  class: TaskPython
```

It has a task name (`task_python` here), and then defines the following mandatory attributes:

- `type`: the task type, this needs to be one the the task types supported by SAYN.
- `module`: the name of the file **within the python folder of the project's root. Please make sure you do not add the .py extension as the module will be imported**.
- `class`: the name of the class in the module which should be ran.

## Writing A `python` Task

### Basics

`python` tasks are used in the following way:

- import the `PythonTask` class from `sayn`.
- define the class you will want the task to run. The task should inherit from `PythonTask`.
- the class you define should have a `setup` and a `run` method. Those methods need to follow a specific signature and **you should only pass `self` as argument**.
- `setup` should return `self.failed()` if an error has occurred otherwise `self.ready()`.
- `run` should return `self.failed()` if an error has occurred otherwise `self.finished()`.

**Important:** for `python` tasks you need to make sure the `python` folder in which you store your Python tasks' code contains an `__init__.py` file.

Please see below some example code for a `python` task:

```python
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
          return self.finished()
```

### Using the SAYN API

In order, to make your `python` tasks dynamic based on project settings and profiles, you can use the SAYN API. A lot of useful information is stored on the task in the `sayn_config` attribute:

- `self.sayn_config.parameters`: accesses the parameters available for the task. Those include the project parameters (`models.yaml`, `settings.yaml`) and additional parameters that might be set at the task's level. For more details on `parameters`, see the parameters section.
- `self.sayn_config.credentials`: accesses the credentials available for the profile used at run time. For more information on `credentials` please see the settings section.
- `self.default_db`: accesses the `default_db` specified in the `models.yaml`.

Those are extremely useful in order to tailor your `python` tasks' code in order to separate between development and production environments.
