# `python` Task

## About

The `python` task allows you run python scripts. Therefore, those tasks can do anything Python can
do. They are extremely useful for data extraction or data science models.

## Defining `python` Tasks

A `python` task is defined as follows:

!!! example "dags/base.yaml"
    ```yaml
    task_python:
      type: python
      class: file_name.ClassName
    ```

Where `class` is a python path to the Python class implementing the task. This code should be stored in the
`python` of your project, which in itself is a python module that's dynamically loaded, so it needs an empty
`__init__.py` file in the folder.

## Writing A `python` Task

### Basics

The basic code to construct a python task is:

!!! example "python/file_name.py"
    ``` python
    from sayn import PythonTask

    class ClassName(PythonTask):
        def setup(self):
            # Do some checked
            return self.success()

        def run(self):
            # Do something useful

            return self.success()
    ```

In this example:
* We create a new class inheriting from SAYN's PythonTask.
* We define a setup method to do some sanity checks. This method can be skipped, but it's
  useful to check the validity of project parameters or so some initial setup.
* We define the actual process to execute during `sayn run` with the `run` method.
* Both `setup` and `run` return the task status as successful `return self.success()`, however
  we can indicate a task failure to sayn with `return self.fail()`. Failing a python task
  forces child tasks to be skipped.

### Using the SAYN API

When defining our python task, you would want to access parts of the SAYN infrastructure like
parameters and connections. Here's a list of properties available:

* `self.parameters`: accesses project and task parameters. For more details on `parameters`,
  see the [Parameters](../parameters.md) section.
* `self.run_arguments`: provides access to the arguments passed to the `sayn run` command like
  the incremental values (`full_load`, `start_dt` and `end_dt`).
* `self.connections`: dictionary containing the databases and other custom API credentials. API
  connections appear as simple python dictionaries, while databases are SAYN's [Database](../api/database.md)
  objects.
* `self.default_db`: provides access to the `default_db` database object specified in the
  `project.yaml` file.

### Logging for Python tasks with the SAYN API

The unit of process within a task in SAYN is the `step`. Using steps is useful to indicate current
progress of execution but also for debugging purposes. The [tutorial](../tutorials/tutorial_part1.md)
is a good example of usage, as we define the `load_data` task as having 5 steps:

!!! example "python/load_data.py"
    ```python
       self.set_run_steps(
           [
               "Generate Data",
               "Load fighters",
               "Load arenas",
               "Load tournaments",
               "Load battles",
           ]
       )
    ```

This code defines which steps form the task. Then we can define the start and end
of that step with:

!!! example "python/load_data.py"
    ```python
    with self.step('Generate Data'):
        data_to_load = get_data(tournament_battles)
    ```

Which will output the following on the screen:

!!! example "CLI output"
    ```bash
    [1/7] load_data (started at 15:25): Step [1/5] Generate Data
    ```

The default cli presentation will show only the current step being executed, which in the
case of the tutorial project goes very quickly. However we can persist these messages using
the debug flag to the cli `sayn run -d` giving you this:

!!! example "CLI ouput"
    ```bash
    [1/7] load_data (started at 15:29)
      Run Steps: Generate Data, Load fighters, Load arenas, Load tournaments, Load battles
      ℹ [1/5] [15:29] Executing Generate Data
      ✔ [1/5] [15:29] Generate Data (19.5ms)
      ℹ [2/5] [15:29] Executing Load fighters
      ✔ [2/5] [15:29] Load fighters (16.9ms)
      ℹ [3/5] [15:29] Executing Load arenas
      ✔ [3/5] [15:29] Load arenas (12.3ms)
      ℹ [4/5] [15:29] Executing Load tournaments
      ✔ [4/5] [15:29] Load tournaments (10.9ms)
      ℹ [5/5] [15:29] Executing Load battles
      ✔ [5/5] [15:29] Load battles (210.3ms)
    ✔ Took (273ms)
    ```

So you can see the time it takes to perform each step.

Sometimes it's useful to output some extra text beyond steps. In those cases, the API provides
some methods for a more adhoc logging model:

* `self.debug(text)`: debug log to console and file. Not printed unless `-d` is used.
* `self.info(text)`: info log to console and file. Not persisted to the screen if `-d` is not specified.
* `self.warning(text)`: warning log to console and file. Remains on the screen after the task finishes (look for yellow lines).
* `self.error(text)`: error log to console and file. Remains on the screen after the task finishes (look for red lines).

!!! note
    `self.error` doesn't abort the execution of the task, nor it sets the final status to being failed.
    To indicate a python task has failed, use this construct: `return self.fail(text)` where text
    is an optional message string that will be showed on the screen.

For more details on the SAYN API, check the [API reference page](../api/python_task.md).
