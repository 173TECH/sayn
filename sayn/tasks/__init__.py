from enum import Enum

from jinja2 import Environment, BaseLoader, StrictUndefined

from .dummy import DummyTask
from .python import PythonTask
from .sql import SqlTaskRunner
from .autosql import AutoSqlTaskRunner
from .copy import CopyTaskRunner


class TaskStatus(Enum):
    UNKNOWN = -1
    SETTING_UP = 0
    READY = 1
    EXECUTING = 2
    SUCCESS = 3
    FAILED = 4
    SKIPPED = 5
    IGNORED = 6


class Task:
    name = None
    dag = None
    tags = list()
    parents = list()
    parameters = dict()
    runner = None
    in_query = True
    status = TaskStatus.UNKNOWN

    # _parent_names = list()
    # _type = None

    def __init__(
        self, task_info, parents, in_query, project_parameters, connections, logger
    ):
        logger._set_config(task_name=task_info["name"])
        self._info = task_info

        self.name = task_info["name"]
        self.dag = task_info["dag"]

        self._type = task_info.get("type")
        self.tags = task_info.get("tags", list())
        self.parents = parents
        self.parameters = task_info.get("parameters", dict())

        self.connections = connections
        self.logger = logger

        self.in_query = in_query

        if not in_query:
            return
        else:
            runner_config = {
                k: v
                for k, v in task_info.items()
                if k not in ("name", "type", "tags", "dag", "parents", "parameters")
            }
            self.runner = self._get_runner(runner_config, project_parameters)

    def _get_runner(self, runner_config, project_parameters):
        creators = {
            "dummy": DummyTask,
            "sql": SqlTaskRunner,
            "autosql": AutoSqlTaskRunner,
            "copy": CopyTaskRunner,
            "python": PythonTask,
        }

        if self._type not in creators:
            raise ValueError(f'"{self._type}" is not a valid task type')
        else:
            runner = creators[self._type]()
            runner.name = self.name
            runner.dag = self.dag
            runner.tags = self.tags
            runner.parameters = dict(**project_parameters, **self.parameters)
            runner.connections = self.connections
            runner.logger = self.logger

            runner.jinja_env = Environment(
                loader=BaseLoader, undefined=StrictUndefined, keep_trailing_newline=True
            )
            runner.jinja_env.globals.update(task=self, **self.parameters)

            runner.setup(**runner_config)

            return runner

    def run(self):
        if self.in_query:
            self.logger.info(f"Running {self.name}...")

    def compile(self):
        if self.in_query:
            self.logger.info(f"Compiling {self.name}...")
