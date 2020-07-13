import importlib

from ..utils.ui import UI
from .task import Task
from .ignore import IgnoreTask
from .dummy import DummyTask
from .python import PythonTask
from .sql import SqlTask
from .autosql import AutoSqlTask
from .copy import CopyTask


def create_python(name, task):
    def fail_creation(name, task):
        task_object = PythonTask(name, task)
        task_object.failed()
        return task_object

    class_str = None
    if "preset" in task:
        if "preset" in task["preset"]:
            if "class" in task["preset"]["preset"]:
                class_str = task["preset"]["preset"].pop("class")

        if "class" in task["preset"]:
            class_str = task["preset"].pop("class")

    if "class" in task:
        class_str = task.pop("class")

    if class_str is None:
        UI()._error('Missing required "class" field in python task')
        return fail_creation(name, task)

    module_str = ".".join(class_str.split(".")[:-1])
    class_str = class_str.split(".")[-1]

    if len(module_str) > 0:
        try:
            task_module = importlib.import_module(f"sayn_python_tasks.{module_str}")
        except Exception as e:
            UI()._error(f'Error loading module "{module_str}"')
            UI()._error(f"{e}")
            return fail_creation(name, task)
    else:
        task_module = importlib.import_module("sayn_python_tasks")

    try:
        klass = getattr(task_module, class_str)
    except Exception as e:
        module_file_name = (
            module_str.replace(".", "/") if len(module_str) > 0 else "__init__"
        )
        UI()._error(
            f'Error importing class "{class_str}" found in "python/{module_file_name}.py"'
        )
        UI()._error(f"{e}")

        return fail_creation(name, task)

    # Create object and ensure there's no extra properties
    task_object = klass(name, task)
    task_object._check_extra_fields()
    return task_object


def create_task(name, type, task, ignore):
    UI()._set_config(task_name=name)

    creators = {
        "ignore": IgnoreTask,
        "dummy": DummyTask,
        "sql": SqlTask,
        "autosql": AutoSqlTask,
        "copy": CopyTask,
        "python": create_python,
    }

    if ignore:
        return creators["ignore"](name, task)
    else:
        if type not in creators:
            UI()._error(f'"{type}" is not a valid task type')
            return Task(name, task)
        return creators[type](name, task)
