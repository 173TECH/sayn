from copy import deepcopy
import os
from typing import List, Dict, Any, Set

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..core.errors import Err, Exc, Ok, Result
from ..utils.misc import map_nested

from .task import FailedTask, Task, TaskStatus
from .dummy import DummyTask
from .sql import SqlTask
from .autosql import AutoSqlTask
from .copy import CopyTask
from .test import TestTask

_creators = {
    "dummy": DummyTask,
    "sql": SqlTask,
    "autosql": AutoSqlTask,
    "copy": CopyTask,
    "test": TestTask,
}

_excluded_properties = (
    "name",
    "type",
    "tags",
    "group",
    "parents",
    "sources",
    "outputs",
    "parameters",
    "class",
    "preset",
    "on_fail",
)


class TaskWrapper:
    """Task wrapper managing the execution of tasks.

    This class wraps a task runner (sayn.tasks.Task) and manages the lifetime of said task.
    In a SAYN execution there will be 1 TaskWrapper per task in the project, whether the task
    is to be executed or ignored based on the task query (`-t` and `-x` arguments to `sayn`).

    Properties:
      name (str): the name of the task
      group (str): the name of the task group file where the task was defined
      tags (List[str]): list of tags declared for the task
      parents (List[sayn.task_wrapper.TaskWrapper]): the list of parents to the current task
      project_parameters (Dict[str, Any]): parameters defined at project level
      task_parameters (Dict[str, Any]): parameters defined at task level
      in_query (bool): whether the task is selected for execution based on the task query
      runner (Task): the object that will do the actual work
      status (TaskStatus): the current status of the task

    """

    name: str
    group: str
    tags: List[str] = None
    parent_names: Set[str] = None
    sources_yaml: List[Any] = None
    outputs_yaml: List[Any] = None
    project_parameters: Dict[str, Any] = None
    task_parameters: Dict[str, Any] = None
    in_query: bool = True
    runner: Task = None
    status: TaskStatus = TaskStatus.UNKNOWN

    def __init__(
        self,
        group: str,
        name: str,
        task_type: str,
        task_parameters: Dict[str, Any],
        on_fail: str,
        parent_names: Set[str],
        sources: Set[str],
        outputs: Set[str],
        tags: Set[str],
        config: Dict[str, Any],
        tracker,
        task_class,
    ):
        self.tags = set(tags or set())
        self.parent_names = set(parent_names or set())
        self.parents = list()
        self.sources_yaml = set(sources or set())
        self.source = set()
        self.outputs_yaml = set(outputs or set())
        self.outputs = set()
        self.tracker = tracker

        self.name = name
        self.group = group
        self.task_type = task_type
        self.on_fail = on_fail or "skip"

        self.task_config = {
            k: v for k, v in config.items() if k not in _excluded_properties
        }
        self.task_parameters = task_parameters or dict()

        self.task_class = task_class

        if isinstance(task_class, FailedTask):
            self.status = TaskStatus.FAILED

        self.runner = None

    def config(
        self,
        connections,
        default_db,
        project_parameters,
        db_obj_stringify,
        run_arguments,
    ):
        self.default_db = default_db
        self.connections = connections

        if self.status == TaskStatus.FAILED:
            return Err("task", "task_creation_error")
        else:
            self.status = TaskStatus.CONFIGURING

        result = self.create_runner(
            self.task_class,
            self.task_config,
            run_arguments,
            project_parameters,
            db_obj_stringify,
            default_db,
            connections,
        )

        if result.is_err:
            return result
        else:
            self.runner = result.value[0]
            runner_config = result.value[1]

        # convert sources and outputs from strings in yaml
        self.outputs = {
            self.obj(o, connection=connections[default_db]) for o in self.outputs_yaml
        }
        self.sources = {
            self.obj(o, connection=connections[default_db]) for o in self.sources_yaml
        }

        try:
            result = self.runner.config(**runner_config)
            return result
        except Exception as e:
            return Exc(e)

    def check_skip(self):
        failed_parents = {
            p.name: p.status
            for p in self.parents
            if (p.status == TaskStatus.FAILED and p.on_fail != "no_skip")
            or p.status == TaskStatus.SKIPPED
        }

        if len(failed_parents) > 0:
            self.status = TaskStatus.SKIPPED

            if self.in_query:
                return Err("task", "parent_errors", failed_parents=failed_parents)
            else:
                return Ok(self.status)

        else:
            return Ok(self.status)

    def setup(self, in_query):
        self.in_query = in_query
        self.status = TaskStatus.SETTING_UP

        # Check the parents are in a good state
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        if not in_query:
            self.status = TaskStatus.NOT_IN_QUERY
            return Ok()
        else:
            # Run the setup stage for the runner and return the results
            try:
                result = self.runner.setup()

            except Exception as exc:
                result = Exc(exc)

            finally:
                if not isinstance(result, Result):
                    self.status = TaskStatus.FAILED
                    self.tracker.current_step = None
                    return Err("task_result", "missing_result_error", result=result)
                elif result.is_err:
                    self.status = TaskStatus.FAILED
                    self.tracker.current_step = None
                    return result
                else:
                    self.status = TaskStatus.READY
                    return Ok()

    def create_runner(
        self,
        task_class,
        runner_config,
        run_arguments,
        project_parameters,
        db_obj_stringify,
        default_db,
        connections,
    ):

        runner = task_class()
        # Add the basic properties
        runner.name = self.name
        runner.group = self.group
        runner.tags = self.tags
        runner.run_arguments = run_arguments
        env_arguments = {
            "full_load": run_arguments["full_load"],
            "start_dt": f"'{run_arguments['start_dt'].strftime('%Y-%m-%d')}'",
            "end_dt": f"'{run_arguments['end_dt'].strftime('%Y-%m-%d')}'",
        }

        # Process parameters
        # The project parameters go as they come
        runner.project_parameters = deepcopy(project_parameters or dict())

        # Create a jinja environment with the project parameters so that we
        # can use that to compile parameters and other properties
        jinja_env = Environment(
            loader=FileSystemLoader(os.getcwd()),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )
        jinja_env.globals.update(task=self)
        jinja_env.globals.update(**env_arguments)
        jinja_env.globals.update(**runner.project_parameters)
        # Compile nested dictionary of parameters
        try:
            runner.task_parameters = map_nested(
                self.task_parameters,
                lambda x: jinja_env.from_string(x).render()
                if isinstance(x, str)
                else x,
            )
        except Exception as e:
            return Exc(e, where="compile_task_parameters")

        # Add the task paramters to the jinja environment
        jinja_env.globals.update(**runner.task_parameters)
        self.jinja_env = jinja_env
        runner.jinja_env = jinja_env
        runner.src = self.src
        runner.out = self.out

        # TODO change this
        runner._wrapper = self

        self.stringify = {
            key: "{{ " + key + " }}" for key in ("database", "schema", "table")
        }

        for obj_type in ("database", "schema", "table"):
            if db_obj_stringify.get(f"{obj_type}_stringify") is not None:
                self.stringify[obj_type] = db_obj_stringify[f"{obj_type}_stringify"]
            else:
                if db_obj_stringify.get(f"{obj_type}_prefix") is not None:
                    self.stringify[obj_type] = (
                        db_obj_stringify[f"{obj_type}_prefix"]
                        + "_"
                        + self.stringify[obj_type]
                    )
                if db_obj_stringify.get(f"{obj_type}_suffix") is not None:
                    self.stringify[obj_type] = (
                        self.stringify[obj_type]
                        + "_"
                        + db_obj_stringify[f"{obj_type}_suffix"]
                    )

        self.stringify = {
            k: jinja_env.from_string(v) for k, v in self.stringify.items()
        }

        # Now we can compile the other properties
        try:
            runner_config = map_nested(
                runner_config,
                lambda x: runner.jinja_env.from_string(x).render()
                if isinstance(x, str)
                else x,
            )
        except Exception as e:
            return Exc(e, where="compile_task_properties")

        # Connections and logging
        runner._default_db = default_db
        runner.connections = connections
        runner.tracker = self.tracker

        return Ok((runner, runner_config))

    def should_run(self):
        return self.status == TaskStatus.NOT_IN_QUERY

    def run(self):
        return self.execute_task("run")

    def compile(self):
        return self.execute_task("compile")

    def test(self):
        return self.execute_task("test")

    def execute_task(self, command):
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        if not self.in_query:
            # TODO review this as it should never happend if running sayn cli
            self.status = TaskStatus.NOT_IN_QUERY
            return Err("execution", "task_not_in_query")
        elif self.status not in (TaskStatus.SETTING_UP, TaskStatus.READY):
            return Err("execution", "setup_error", status=self.status)
        else:
            try:
                if command == "run":
                    result = self.runner.run()
                elif command == "compile":
                    result = self.runner.compile()
                else:
                    result = self.runner.test()
                if result is None:
                    self.status = TaskStatus.FAILED
                    result = Err("execution", "none_return_value")
                if result.is_ok:
                    self.status = TaskStatus.SUCCEEDED
                else:
                    self.status = TaskStatus.FAILED
            except Exception as e:
                self.status = TaskStatus.FAILED
                result = Exc(e)

            return result

    def set_parents(self, all_tasks, output_to_task):
        for parent_name in self.parent_names:
            self.parents.append(all_tasks[parent_name])
            self.parent_names.add(parent_name)

        for source in self.sources:
            for task_name in output_to_task[source]:
                if task_name not in self.parent_names:
                    self.parents.append(all_tasks[task_name])
                    self.parent_names.add(task_name)

    def obj(self, *args, connection=None):
        args = [a for a in args if a is not None]
        full = ".".join(args)
        args = full.split(".")

        database = None
        schema = None
        table = None

        if len(args) == 1:
            table = args[0]
        elif len(args) == 2:
            schema = args[0]
            table = args[1]
        elif len(args) == 3:
            database = args[0]
            schema = args[1]
            table = args[2]
        else:
            raise ValueError("Too many arguments")

        if database is not None:
            database = self.jinja_env.from_string(database).render()

        if schema is not None:
            schema = self.jinja_env.from_string(schema).render()

        if table is not None:
            table = self.jinja_env.from_string(table).render()

        if connection is None:
            connection = self.connections[self.default_db]
        elif isinstance(connection, str):
            connection = self.connections[connection]

        obj = connection.get_db_object(
            database, schema, table, self.stringify, self.stringify
        )

        return obj

    def src(self, obj, connection=None):
        obj = self.obj(obj, connection=connection)
        self.sources.add(obj)
        return obj.get_value()

    def out(self, obj, connection=None):
        obj = self.obj(obj, connection=connection)
        self.outputs.add(obj)
        return obj.get_value()
