from copy import deepcopy
import os
from typing import List, Dict, Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..core.errors import Err, Exc, Ok, Result
from ..utils.misc import map_nested

from . import Task, TaskStatus
from .dummy import DummyTask
from .sql import SqlTask
from .autosql import AutoSqlTask
from .copy import CopyTask

_creators = {
    "dummy": DummyTask,
    "sql": SqlTask,
    "autosql": AutoSqlTask,
    "copy": CopyTask,
}

_excluded_properties = (
    "name",
    "type",
    "tags",
    "dag",
    "parents",
    "parameters",
    "class",
    "preset",
)


class TaskWrapper:
    """Task wrapper managing the execution of tasks.

    This class wraps a task runner (sayn.tasks.Task) and manages the lifetime of said task.
    In a SAYN execution there will be 1 TaskWrapper per task in the project, whether the task
    is to be executed or ignored based on the task query (`-t` and `-x` arguments to `sayn`).

    Properties:
      name (str): the name of the task
      dag (str): the name of the dag file where the task was defined
      tags (List[str]): list of tags declared for the task
      parents (List[sayn.task_wrapper.TaskWrapper]): the list of parents to the current task
      project_parameters (Dict[str, Any]): parameters defined at project level
      task_parameters (Dict[str, Any]): parameters defined at task level
      in_query (bool): whether the task is selected for execution based on the task query
      runner (Task): the object that will do the actual work
      status (TaskStatus): the current status of the task

    """

    name: str = None
    dag: str = None
    tags: List[str] = list()
    parents: List[Any] = list()
    project_parameters: Dict[str, Any] = dict()
    task_parameters: Dict[str, Any] = dict()
    in_query: bool = True
    runner: Task = None
    status: TaskStatus = TaskStatus.UNKNOWN

    def check_skip(self):
        failed_parents = {
            p.name: p.status
            for p in self.parents
            if p.status in (TaskStatus.SKIPPED, TaskStatus.FAILED)
        }

        if len(failed_parents) > 0:
            self.status = TaskStatus.SKIPPED

            if self.in_query:
                return Err("task", "parent_errors", failed_parents=failed_parents)
            else:
                return Ok(self.status)

        else:
            return Ok(self.status)

    def setup(
        self,
        task_info,
        parents,
        in_query,
        tracker,
        connections,
        default_db,
        project_parameters,
        run_arguments,
        python_loader,
    ):
        def failed(result):
            self.status = TaskStatus.FAILED
            self.tracker.current_step = None
            return result

        def setup_runner(runner, runner_config):
            try:
                result = runner.setup(**runner_config)

            except Exception as exc:
                result = Exc(exc)

            finally:
                if not isinstance(result, Result):
                    return failed(
                        Err("task_result", "missing_result_error", result=result)
                    )
                elif result.is_err:
                    return failed(result)
                else:
                    self.runner = runner
                    self.status = TaskStatus.READY
                    return Ok()

        self.tracker = tracker

        self.status = TaskStatus.SETTING_UP
        self._info = task_info

        self.name = task_info["name"]
        self.dag = task_info["dag"]

        self._type = task_info.get("type")
        self.tags = task_info.get("tags", list())
        self.parents = parents

        self.task_parameters = task_info.get("parameters", dict())

        self.in_query = in_query

        # Check the parents are in a good state
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        if not in_query:
            self.status = TaskStatus.NOT_IN_QUERY
            return Ok()
        else:

            # Instantiate the Task runner object

            # First get the class to use
            if self._type == "python":
                result = python_loader.get_class("python_tasks", task_info.get("class"))
                if result.is_err:
                    # TODO should probably add the context here
                    return failed(result)
                else:
                    task_class = result.value

            elif self._type in _creators:
                task_class = _creators[self._type]

            else:
                return failed(
                    Err(
                        "task_type",
                        "invalid_task_type_error",
                        type=self._type,
                        dag=self.dag,
                    )
                )

            # Call the object creation method
            result = self.create_runner(
                task_class,
                {k: v for k, v in task_info.items() if k not in _excluded_properties},
                run_arguments,
                project_parameters,
                default_db,
                connections,
            )
            if result.is_ok:
                runner, runner_config = result.value
            else:
                return failed(result)

            # Run the setup stage for the runner and return the results
            return setup_runner(runner, runner_config)

    def create_runner(
        self,
        task_class,
        runner_config,
        run_arguments,
        project_parameters,
        default_db,
        connections,
    ):
        runner = task_class()

        # Add the basic properties
        runner.name = self.name
        runner.dag = self.dag
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
        runner.jinja_env = jinja_env

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
                else:
                    result = self.runner.compile()
                if result.is_ok:
                    self.status = TaskStatus.SUCCEEDED
                else:
                    self.status = TaskStatus.FAILED
            except Exception as e:
                self.status = TaskStatus.FAILED
                result = Exc(e)

            return result
