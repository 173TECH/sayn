from enum import Enum
import logging

from ..config import Config


class TaskStatus(Enum):
    UNKNOWN = -1
    SETTING_UP = 0
    READY = 1
    EXECUTING = 2
    FINISHED = 3
    FAILED = 4
    SKIPPED = 5
    IGNORED = 6


class Task(object):
    # Init functions
    def __init__(self, name, task, group, model):
        self.sayn_config = Config()

        self.name = name
        self.model = model

        self._task_def_start_line = task.start_line
        self._task_def_end_line = task.end_line
        if group is not None:
            self._group_def_start_line = group.start_line
            self._group_def_end_line = group.end_line
        else:
            self._group_def_start_line = None

        _task_def = task.data
        if group is not None:
            # Merge group into task definition
            for prop, val in group.data.items():
                if prop not in _task_def:
                    _task_def[prop] = val
                elif prop == "parameters":
                    for name, param in val.items():
                        if name not in _task_def["parameters"]:
                            _task_def["parameters"][name] = val[name]
                elif prop == "parents":
                    if not isinstance(val, list):
                        self.failed(
                            f"Parents need to be a list in group {_task_def['group']}"
                        )
                        return
                    _task_def["parents"] = list(
                        set(_task_def.get("parents", list()) + val)
                    )

        self.type = _task_def.pop("type")
        self.group = _task_def.pop("group", None)
        self.tags = _task_def.pop("tags", list())
        self.parents = _task_def.pop("parents", list())

        self.parameters = _task_def.pop("parameters", dict())
        for name, value in self.parameters.items():
            self.parameters[name] = self.compile_property(self.parameters[name])

        self._task_def = _task_def

        self.setting_up()

    def set_parents(self, tasks):
        """Replaces the parents list with the task objects"""
        self.parents = [tasks[p] for p in self.parents]

    # Utility functions
    def compile_property(self, value):
        if value is None:
            return
        if isinstance(value, str):
            return Config().jinja_env.from_string(value).render(task=self)
        elif isinstance(value, list):
            return [self.compile_property(i) for i in value]
        elif isinstance(value, dict):
            return {k: self.compile_property(v) for k, v in value.items()}
        else:
            logging.error(
                "Property value type {} not supported".format(str(type(value)))
            )

    # Execution functions

    def set_current(self):
        self.sayn_config.set_current_task(self.name)

    def should_run(self):
        # Initially all tasks should run, except IgnoreTask
        return True

    def can_run(self):
        if self.status != TaskStatus.READY:
            return False
        for p in self.parents:
            if p.status not in (TaskStatus.IGNORED, TaskStatus.FINISHED):
                return False
        return True

    # API

    def setup(self):
        return self.failed()

    def run(self):
        return self.failed()

    def compile(self):
        return self.failed()

    # Status functions

    def setting_up(self):
        self.status = TaskStatus.SETTING_UP
        return self.status

    def ready(self):
        self.status = TaskStatus.READY
        return self.status

    def executing(self):
        self.status = TaskStatus.EXECUTING
        return self.status

    def finished(self):
        self.status = TaskStatus.FINISHED
        return self.status

    def failed(self, messages=None):
        if messages is not None:
            if isinstance(messages, str):
                messages = [messages]
            for message in messages:
                logging.error(message)

        self.status = TaskStatus.FAILED
        return self.status

    def skipped(self):
        self.status = TaskStatus.SKIPPED
        return self.status
