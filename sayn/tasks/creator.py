from enum import Enum
import importlib

from jinja2 import Environment, BaseLoader, StrictUndefined

from ..utils.ui import UI

from .dummy import DummyTask
from .python import PythonTask
from .sql import SqlTaskRunner
from .autosql import AutoSqlTask
from .copy import CopyTask


def create_python(name, task):
    def fail_creation(name, task):
        task_object = PythonTask(name, task)
        task_object.failed()
        return task_object

    class_str = None
    if "class" in task:
        class_str = task.pop("class")

    if class_str is None:
        UI().error('Missing required "class" field in python task')
        return fail_creation(name, task)

    module_str = ".".join(class_str.split(".")[:-1])
    class_str = class_str.split(".")[-1]

    if len(module_str) > 0:
        try:
            task_module = importlib.import_module(f"sayn_python_tasks.{module_str}")
        except Exception as e:
            UI().error(f'Error loading module "{module_str}"')
            UI().error(f"{e}")
            return fail_creation(name, task)
    else:
        task_module = importlib.import_module("sayn_python_tasks")

    try:
        klass = getattr(task_module, class_str)
    except Exception as e:
        module_file_name = (
            module_str.replace(".", "/") if len(module_str) > 0 else "__init__"
        )
        UI().error(
            f'Error importing class "{class_str}" found in "python/{module_file_name}.py"'
        )
        UI().error(f"{e}")

        return fail_creation(name, task)

    # Create object and ensure there's no extra properties
    task_object = klass(name, task)
    task_object._check_extra_fields()
    return task_object


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
            "autosql": AutoSqlTask,
            "copy": CopyTask,
            "python": create_python,
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
