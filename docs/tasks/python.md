# `python` Task

## About

The `python` task allows you to run python scripts. Therefore, those tasks can do anything Python can
do. They are extremely useful for data extraction or data science models.

There are two models for specifying python tasks in SAYN: a simple way through using decorators and a more advanced way which is class based.

## Simple Definition of `python` Tasks

You can define `python` tasks in SAYN very simply by using decorators. This will let you write a Python function and turn that function into a task. First, you need to add a group in `project.yaml` pointing to the `.py` file where the task code lives:

!!! example "project.yaml"
    ```
    groups:
      decorator_tasks:
        type: python
        module: decorator_tasks
        parameters:
          param1: some_value
    ```

Now all tasks defined in `python/decorator_tasks.py` will be added to the DAG. The `module` property expects a python path from the `python` folder in a similar way as you would import a module in python. For example, if our task definition exists in `python/example_mod/decorator_tasks.py` the value in `module` would be `example_mod.decorator_tasks`.

!!! example "python/decorator_tasks.py"
    ```
    from sayn import task

    @task(outputs='logs.api_table', sources='logs.another_table')
    def example_task(context, warehouse, param1):
        src_table = context.src('logs.another_table')
        out_table = context.out('logs.api_table')
        warehouse.execute(f'CREATE OR REPLACE TABLE {out_table} AS SELECT * from {src_table}')
    ```

The above example showcases the key elements to a python task:

  * `task`: we import SAYN's `task` decorator which is used to turn functions into SAYN tasks added to the DAG.
  * parameters to `task`: we can pass parameters `sources`, `outputs` and `parents` which are either lists of table names or a single table name. This allows SAYN define the task dependencies. We can also pass a value for `on_fail` and `tags`.
  * function name: the name of the function (`example_task` here) will be the name of the task. We can use this name with `-t` to execute this task only for example.
  * function parameters: arguments to the function have special meaning and so the names need to be respected:
    * `context`: is an object granting access to some functionality like project parameters, connections and other functions as seen further down.
    * `warehouse`: connection names (`required_credentials` in `project.yaml`) will automatically provide the object of that connection. You can specify any number of connections here.
    * param1: the rest of the function arguments are matched against task parameters, these are values defined in the `parameter` property in the group.

!!! info "Python decorators"
    Decorators in python are used to modify the behaviour of a function. It can be a bit daunting to understand when we first encounter them but for the purpose of SAYN all you need to know is that `@task` turns a standard python
    function into a SAYN task which can assess useful properties via arguments. There are many resources online describing how decorators work,
    [for example this](https://realpython.com/primer-on-python-decorators/).

Given the code above, this task will:

  * Depend on (execute after) the tasks that produce `logs.another_table` since we added the `sources` argument to the decorator.
  * Be the parent of (execute before) any task reading from `logs.api_table` since we added the `outputs` argument to the decorator.
  * Get the compiled value of `logs.another_table` and `logs.api_table` and keep in 2 variables. For details on database objects compilation
    make sure you check [the database objects page](../database_objects.md).
  * Execute a create table statement using the tables above on the database called `warehouse` in the project.

## Advanced `python` Task Definition With Classes

The second model for defining python tasks is through classes. When using this model we get an opportunity to:

  * do validation before the task is executed by overloading the `setup` method, which is useful as a way to alert early during the execution that something is incorrectly defined rather than waiting for the task to fail.
  * define more complex dependencies than `sources` and `outputs` by overloading the `config` method.
  * implement code for the `compile` stage allowing for more early stage indication of problems.

A `python` task using classes is defined as follows:

!!! example "tasks/base.yaml"
    ```yaml
    task_python:
      type: python
      class: file_name.ClassName
    ```

Where `class` is a python path to the Python class implementing the task. This code should be stored in the `python` folder of your project, which in itself is a python module that's dynamically loaded, so it needs an empty `__init__.py` file in the folder. The class then needs to be defined as follows:

!!! example "python/file_name.py"
    ``` python
    from sayn import PythonTask

    class ClassName(PythonTask):
        def config(self):
            self.src('logs.source_table')
            self.out('logs.output_table')

        def setup(self):
            # Do some validation of the parameters
            return self.success()

        def run(self):
            # Do something useful

            return self.success()
    ```

In this example:

* We create a new class inheriting from SAYN's PythonTask.
* We set some dependencies by calling `self.src` and `self.out`.
* We define a setup method to do some sanity checks. This method can be skipped, but it's
  useful to check the validity of project parameters or so some initial setup.
* We define the actual process to execute during `sayn run` with the `run` method.
* Both `setup` and `run` return the task status as successful `return self.success()`, however we can indicate a task failure to sayn with `return self.fail()`. Failing a python task forces child tasks to be skipped.

???+ attention
     Python tasks can return `self.success()` or `self.fail()` to indicate the result of the execution, but it's not mandatory. If the code throws a python exception, the task will be considered as failed.

## Using the SAYN API

When defining our `python` task, you would want to access parts of the SAYN infrastructure like
parameters and connections. When using the decorator model, to access this functionality we need to include the `context` argument in the function, when using the class model the more standard `self` is used, and both give access to the same functionality. The list of available properties through `self` and `context` is:

* `parameters`: accesses project and task parameters. For more details on `parameters`,
  see the [Parameters](../parameters.md) section.
* `run_arguments`: provides access to the arguments passed to the `sayn run` command like the incremental values (`full_load`, `start_dt` and `end_dt`).
* `connections`: dictionary containing the databases and other custom API credentials. API connections appear as simple python dictionaries, while databases are SAYN's [Database](../api/database.md) objects.
* `default_db`: provides access to the `default_db` database object specified in the `project.yaml` file.
* `src`: the `src` macro that translates database object names as described in [database objects](../database_objects.md). Bear in mind that using this function also adds dependencies to the task, but only when called from the `config` method of a python task defined with the class model.
* `out`: the `out` macro that translates database object names as described in [database objects](../database_objects.md). Bear in mind that using this function also creates dependencies between tasks, but only when called from the `config` method of a python task defined with the class model.

!!! tip
    You can use `self.default_db` to easily perform some operations on the default database such as reading or
    loading data or if using decorators simply include an argument with the name of the connection. See the
    methods available on the [Database](../api/database.md) API.

!!! tip
    We all love `pandas`! If you want to load a pandas dataframe you can use one of these options:

    * with the `pandas.DataFrame.to_sql` method: `df.to_sql(self.default_db.engine, 'table')`.
    * with the `self.default_db.load_data` method: `self.default_db.load_data('table', df.to_dict('records'))`.

## Logging For `python` Tasks With The SAYN API

The unit of process within a task in SAYN is the `step`. Using steps is useful to indicate current
progress of execution but also for debugging purposes. The [tutorial](../tutorials/tutorial_part3.md) is a good example of usage, as we define the `load_data` task as having 5 steps:

!!! example "python/load_data.py"
    ```python
       context.set_run_steps(
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
    with context.step('Generate Data'):
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
