from enum import Enum

from ..config import Config
from ..utils.ui import UI


class TaskStatus(Enum):
    UNKNOWN = -1
    SETTING_UP = 0
    READY = 1
    EXECUTING = 2
    SUCCESS = 3
    FAILED = 4
    SKIPPED = 5
    IGNORED = 6


class Task(object):
    # Init functions
    def __init__(self, name, task):
        self.sayn_config = Config()

        self.name = name
        self.type = task.pop("type")
        self.dag = task.pop("dag")
        self.tags = task.pop("tags", list())
        self.parents = task.pop("parents", list())

        self.parameters = task.pop("parameters", dict())
        for name, value in self.parameters.items():
            self.parameters[name] = self.compile_property(self.parameters[name])

        self._task_def = task

        self.setting_up()

    def set_parents(self, tasks):
        """Replaces the parents list with the task objects"""
        self.parents = [tasks[p] for p in self.parents]

    # Properties methods

    def _check_extra_fields(self):
        # Cleanup preset
        if "preset" in self._task_def:
            if "preset" in self._task_def["preset"]:
                if len(self._task_def["preset"]["preset"]) == 0:
                    # Clean nested preset if possible
                    del self._task_def["preset"]["preset"]

            if len(self._task_def["preset"]) == 0:
                # If the nested preset is empty, clean the outer one if possible
                del self._task_def["preset"]

        if len(self._task_def) > 0:
            return self.failed(
                "Invalid properties for {}: {}".format(
                    self.type, ", ".join(self._task_def.keys())
                )
            )
        else:
            return self.ready()

    def _pop_property(self, property, default=None):
        """Navigates the task definition and finds the first occurrence of the named property.
           First it checks in the task, then the preset and finally the nested preset of the preset
           (the project.yaml preset).
           Returns None if not found anywhere"""

        def merge_value(value, new_value):
            if value is None:
                return new_value
            elif not isinstance(new_value, type(value)):
                raise ValueError(type(new_value))
            elif isinstance(value, dict):
                value.update(new_value)
            elif isinstance(value, list):
                value.extend([v for v in new_value if v not in value])
            else:
                value = new_value

            return value

        value = default

        # Extract the value from the deepest level of nesting first and go outwards
        # We always remove it from the _task_def
        if "preset" in self._task_def:
            if "preset" in self._task_def["preset"]:
                if property in self._task_def["preset"]["preset"]:
                    new_value = self._task_def["preset"]["preset"].pop(property)
                    value = merge_value(value, new_value)

            if property in self._task_def["preset"]:
                new_value = self._task_def["preset"].pop(property)
                value = merge_value(value, new_value)

        if property in self._task_def:
            new_value = self._task_def.pop(property)
            value = merge_value(value, new_value)

        return value

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
            UI()._error("Property value type {} not supported".format(str(type(value))))

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
            if p.status not in (TaskStatus.IGNORED, TaskStatus.SUCCESS):
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

    def success(self):
        self.status = TaskStatus.SUCCESS
        return self.status

    def failed(self, messages=None):
        if messages is not None:
            if isinstance(messages, str):
                messages = [messages]
            for message in messages:
                UI()._error(message)

        self.status = TaskStatus.FAILED
        return self.status

    def skipped(self):
        self.status = TaskStatus.SKIPPED
        return self.status
