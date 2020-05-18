import importlib
import logging

from ..utils.logger import Logger
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

    return fail_creation(name, task)

    module = task.data.get("module")
    if module is None:
        if 'preset' in task:
            if 'module' in task.preset:
                pass
            module = group.get("module")
        if module is None:
            logging.error(f'Missing module for task "{name}"')
            return fail_creation(name, task, group, model)
        else:
            module = module.data

    class_name = task.data.get("class")
    if class_name is None:
        if group is not None:
            class_name = group.get("class")
        if class_name is None:
            logging.error(f'Missing class for task "{name}"')
            return fail_creation(name, task, group, model)
        else:
            class_name = class_name.data

    try:
        task_module = importlib.import_module(f'python_tasks.{module}')
    except:
        logging.error(f'Module "{module}" not found in python folder')

        return fail_creation(name, task, group, model)

    try:
        klass = getattr(task_module, class_name)
    except:
        logging.error(
            f'No task class "{class_name}" found in "python/{module.replace(".", "/")}.py"'
        )

        return fail_creation(name, task, group, model)

    task_object = klass(name, task, group, model)
    return task_object


def create_task(name, type, task, ignore):
    Logger().set_config(task=name)

    creators = {
        "ignore": IgnoreTask,
        "dummy": DummyTask,
        "sql": SqlTask,
        "autosql": AutoSqlTask,
        "copy": CopyTask,
        # TODO fixes to python tasks
        "python": IgnoreTask, #create_python,
    }

    if ignore:
        return creators["ignore"](name, task)
    else:
        if type not in creators:
            logging.error(f'"{type}" is not a valid task type')
            return Task(name, task)
        return creators[type](name, task)
