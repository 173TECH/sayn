from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


from ..utils.python_loader import PythonLoader
from ..logging.task_event_tracker import TaskEventTracker
from .task_wrapper import TaskWrapper
from .dummy import DummyTask
from .sql import SqlTask
from .autosql import AutoSqlTask
from .copy import CopyTask

# _excluded_properties = (
#     "name",
#     "type",
#     "tags",
#     "group",
#     "parents",
#     "sources",
#     "outputs",
#     "parameters",
#     "class",
#     "preset",
#     "on_fail",
# )

# def get_task_class(self, python_loader, task_type, config):
#     if task_type == "python":
#         return self.python_loader.get_class("python_tasks", config.get("class"))
#     elif task_type in _creators:
#         return Ok(_creators[task_type])
#     else:
#         return Err(
#             "task_type",
#             "invalid_task_type_error",
#             type=task_type,
#             group=config["group"],
#         )


class TaskBuilder:
    _creators = {}

    def __init__(self, python_folder, sql_folder, app_tracker, default_db, connections):
        self.app_tracker = app_tracker
        self.sql_folder = sql_folder
        self.default_db = default_db
        self.connections = connections

        if Path(python_folder).is_dir():
            self.python_loader = PythonLoader()
        else:
            self.python_loader = None

        self.base_jinja_env = Environment(
            loader=FileSystemLoader(str(Path("."))),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

    def get_task(self, config):
        if config.type == "dummy":
            runner_class = DummyTask
        elif config.type == "sql":
            runner_class = SqlTask
        elif config.type == "autosql":
            runner_class = AutoSqlTask
        elif config.type == "copy":
            runner_class = CopyTask
        elif config.type == "python":
            if self.python_loader is None:
                raise ValueError("No python module defined in the project")

        # (config.name, config.group, self.app_tracker.get_task_tracker(config.name), self.run_arguments, config.parameters, self.project_parameters, self.default_db, self.connections)

        return TaskWrapper(runner_class)


class SAYNError(Exception):
    pass
