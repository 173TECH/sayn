from copy import deepcopy

from jinja2 import Environment, BaseLoader, StrictUndefined

from ..core.errors import TaskCreationError

# from ..utils.python_loader import PythonLoader
from . import TaskStatus
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


class TaskWrapper:
    name = None
    dag = None
    tags = list()
    parents = list()
    project_parameters = dict()
    task_parameters = dict()
    in_query = True
    runner = None
    status = TaskStatus.UNKNOWN

    def __init__(
        self,
        task_info,
        parents,
        in_query,
        logger,
        project_parameters,
        connections,
        default_db,
    ):
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
            runner_config = {
                k: v
                for k, v in task_info.items()
                if k
                not in ("name", "type", "tags", "dag", "parents", "parameters", "class")
            }
            print(runner_config)

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
            runner.name = self.name
            runner.dag = self.dag
            runner.tags = self.tags
            runner.project_parameters = deepcopy(project_parameters or dict())
            runner.task_parameters = task_info.get("parameters", dict())
            runner._default_db = default_db
            runner.connections = connections
            runner.logger = self.logger

            runner.jinja_env = Environment(
                loader=BaseLoader,
                undefined=StrictUndefined,
                keep_trailing_newline=True,
            )
            runner.jinja_env.globals.update(task=self, **runner.parameters)

            # TODO
            # try:
            #     runner.setup(**runner_config)
            # except Exception as e:
            #     raise TaskCreationError(f"{e}")

            self.runner = runner

    def run(self):
        if self.in_query:
            self.logger.info(f"Running {self.name}...")

    def compile(self):
        if self.in_query:
            self.logger.info(f"Compiling {self.name}...")
