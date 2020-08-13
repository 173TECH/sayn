from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any
from copy import deepcopy

from jinja2 import Environment, BaseLoader, StrictUndefined
from pydantic import ValidationError

from ..core.errors import TaskCreationError
from ..utils.misc import map_nested

# from ..utils.python_loader import PythonLoader
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
    parents: List[TaskWrapper] = list()
    project_parameters: Dict[str, Any] = dict()
    task_parameters: Dict[str, Any] = dict()
    in_query: bool = True
    runner: Task = None
    status: TaskStatus = TaskStatus.UNKNOWN

    start_ts: None
    end_ts: None

    def __init__(
        self,
        task_info,
        parents,
        in_query,
        logger,
        connections,
        default_db,
        project_parameters,
        run_arguments,
    ):
        self.status = TaskStatus.SETTING_UP
        self._info = task_info

        self.name = task_info["name"]
        self.dag = task_info["dag"]

        self._type = task_info.get("type")
        self.tags = task_info.get("tags", list())
        self.parents = parents

        self.logger = logger

        self.in_query = in_query

        if not in_query:
            self.status = TaskStatus.NOT_IN_QUERY
        else:
            # Instantiate the Task runner object
            if self._type == "python":
                # task_class = PythonLoader().get_class(
                #     "sayn_python_tasks", task_info.get("class")
                # )
                task_class = DummyTask
            elif self._type in _creators:
                task_class = _creators[self._type]
            else:
                raise TaskCreationError(f'"{self._type}" is not a valid task type')

            runner = task_class()

            # Add the basic properties
            runner.name = self.name
            runner.dag = self.dag
            runner.tags = self.tags
            runner.run_arguments = run_arguments
            env_arguments = {
                "full_load": run_arguments["full_load"],
                "start_dt": run_arguments["start_dt"].strftime("%Y-%m-%d"),
                "end_dt": run_arguments["end_dt"].strftime("%Y-%m-%d"),
            }

            # Process parameters
            # The project parameters go as they come
            runner.project_parameters = deepcopy(project_parameters or dict())
            # Create a jinja environment with the project parameters so that we
            # can use that to compile parameters and other properties
            jinja_env = Environment(
                loader=BaseLoader,
                undefined=StrictUndefined,
                keep_trailing_newline=True,
            )
            jinja_env.globals.update(task=self)
            jinja_env.globals.update(**env_arguments)
            jinja_env.globals.update(**runner.project_parameters)
            task_parameters = task_info.get("parameters", dict())
            # Compile nested dictionary of parameters
            runner.task_parameters = map_nested(
                task_parameters,
                lambda x: jinja_env.from_string(x).render()
                if isinstance(x, str)
                else x,
            )
            # Add the task paramters to the jinja environment
            jinja_env.globals.update(**runner.task_parameters)
            runner.jinja_env = jinja_env

            # Now we can compile the other properties
            runner_config = {
                k: v for k, v in task_info.items() if k not in _excluded_properties
            }
            runner_config = map_nested(
                runner_config,
                lambda x: runner.jinja_env.from_string(x).render()
                if isinstance(x, str)
                else x,
            )

            # Connections and logging
            runner._default_db = default_db
            runner.connections = connections
            runner.logger = self.logger

            # Run the setup stage for the runner
            try:
                runner.setup(**runner_config)
                self.runner = runner
                self.status = TaskStatus.READY
            except ValidationError as e:
                self.status = TaskStatus.FAILED
                self.logger.error(f"Error setting up: {e}")
            except Exception as e:
                self.status = TaskStatus.FAILED
                self.logger.error(f"Error setting up: {e}")
                raise e

    # TODO review
    def should_run(self):
        # Initially all tasks should run, except IgnoreTask
        return True

    # TODO review
    def can_run(self):
        if self.status != TaskStatus.READY:
            return False
        for p in self.parents:
            if p.status not in (TaskStatus.IGNORED, TaskStatus.SUCCESS):
                return False
        return True

    def run(self):
        self._execute_task("run")

    def compile(self):
        self._execute_task("compile")

    def _execute_task(self, command):
        self.start_ts = datetime.now()
        self.logger._report_event(
            event=f"start_{command}", level="info", message="Started task"
        )
        if self.in_query:
            if command == "run":
                self.runner.run()
            else:
                self.runner.compile()
        self.end_ts = datetime.now()
        self.logger._report_event(
            event=f"finish_{command}",
            level="success",
            message="Finish task",
            duration=self.end_ts - self.start_ts,
        )
