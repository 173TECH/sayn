from enum import Enum
import importlib

from ..utils.ui import UI
from ..utils.misc import merge_dicts, merge_dict_list
from ..utils.dag import dag_is_valid, upstream, topological_sort
from ..app.config import ConfigError

from .dummy import DummyTask
from .python import PythonTask
from .sql import SqlTask
from .autosql import AutoSqlTask
from .copy import CopyTask


def get_presets(global_presets, dags):
    """Returns a dictionary of presets merged with the referenced preset

    Presets define a direct acyclic graph by including the `preset` property, so
    this function validates that there are no cycles and that all referenced presets
    are defined.

    In the output, preset names are prefixed with `sayn_global:` or `dag:` so that we can
    merge all presets in the project in the same dictionary.

    Args:
      global_presets (dict): dictionary containing the presets defined in project.yaml
      dags (sayn.app.config.Dag): a list of dags from the dags/ folder
    """
    # 1. Construct a dictionary of presets so we can attach that info to the tasks
    presets_info = {
        f"sayn_global:{k}": {kk: vv for kk, vv in v.items() if kk != "preset"}
        for k, v in global_presets.items()
    }

    # 1.1. We start with the global presets defined in project.yaml
    presets_dag = {
        k: [f"sayn_global:{v}"] if v is not None else []
        for k, v in {
            f"sayn_global:{name}": preset.get("preset")
            for name, preset in global_presets.items()
        }.items()
    }

    # 1.2. Then we add the presets defined in the dags
    for dag_name, dag in dags.items():
        presets_info.update(
            {
                f"{dag_name}:{k}": {kk: vv for kk, vv in v.items() if kk != "preset"}
                for k, v in dag.presets.items()
            }
        )

        dag_presets_dag = {
            name: preset.get("preset") for name, preset in dag.presets.items()
        }

        # Check if the preset referenced is defined in the dag, otherwise, point at the
        # global dag
        dag_presets_dag = {
            f"{dag_name}:{k}": [
                f"{dag_name}:{v}" if v in dag_presets_dag else f"sayn_global:{v}"
            ]
            if v is not None
            else []
            for k, v in dag_presets_dag.items()
        }
        presets_dag.update(dag_presets_dag)

    # 1.3. The preset references represent a dag that we need to validate, ensuring
    #      there are no cycles and that all references exists
    dag_is_valid(presets_dag)

    # 1.4. Merge the presets with the reference preset, so that we have 1 dictionary
    #      per preset a task could reference
    presets = {
        name: merge_dict_list(
            [presets_info[p] for p in upstream(presets_dag, name)]
            + [presets_info[name]]
        )
        for name in topological_sort(presets_dag)
    }

    return presets


def get_task_dict(task, task_name, dag_name, presets):
    """Returns a single task merged with the referenced preset

    Args:
      task (dict): a dictionary with the task information
      task_name (str): the name of the task
      dag_name (str): the name of the dag it appeared on
      presets (dict): a dictionary of merged presets returned by get_presets
    """
    if "preset" in task:
        preset_name = task["preset"]
        preset = presets.get(
            f"{dag_name}:{preset_name}", presets.get(f"sayn_global:{preset_name}")
        )
        if preset is None:
            raise ConfigError(
                f'Preset "{preset_name}" referenced by task "{task_name}" in dag "{dag_name}" not declared'
            )
        task = merge_dicts(preset, task)

    return dict(task, name=task_name, dag=dag_name)


def get_tasks_dict(global_presets, dags):
    """Returns a dictionary with the task definition with the preset information merged

    Args:
      global_presets (dict): a dictionary with the presets as defined in project.yaml
      dags (sayn.common.config.Dag): a list of dags from the dags/ folder
    """
    presets = get_presets(global_presets, dags)

    return {
        task_name: get_task_dict(task, task_name, dag_name, presets)
        for dag_name, dag in dags.items()
        for task_name, task in dag.tasks.items()
    }


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
    runner = None
    in_query = True
    status = TaskStatus.UNKNOWN

    # _parent_names = list()
    # _type = None

    # TODO file line numbers

    def __init__(self, task_info, parents, in_query):
        UI()._set_config(task_name=task_info["name"])
        self._info = {
            k: v
            for k, v in task_info.items()
            if k not in ("name", "type", "tags", "dag", "parents")
        }

        self.name = task_info["name"]
        self.dag = task_info["dag"]

        self._type = task_info.get("type")
        self.tags = task_info.get("tags", list())
        self.parents = parents
        self.in_query = in_query

        if in_query:
            creators = {
                "dummy": DummyTask,
                "sql": SqlTask,
                "autosql": AutoSqlTask,
                "copy": CopyTask,
                "python": create_python,
            }

            if self._type not in creators:
                raise ValueError(f'"{self._type}" is not a valid task type')
            else:
                self.runner = creators[self._type](self.name, self._info)

        # TODO check for errors

    def run(self):
        if self.in_query:
            UI().info(f"Running {self.name}...")

    def compile(self):
        if self.in_query:
            UI().info(f"Compiling {self.name}...")
