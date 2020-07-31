from datetime import datetime
import re
from uuid import UUID, uuid4

from ..utils.ui import UI
from ..utils.misc import merge_dicts, merge_dict_list
from ..utils.dag import (
    is_valid as dag_is_valid,
    upstream as dag_upstream,
    topological_sort,
)
from .config import ConfigError


RE_TASK_QUERY = re.compile(
    (
        r"^("
        r"(?!(dag:|tag:))(?P<upstream>\+?)(?P<task>[a-zA-Z0-9][-_a-zA-Z0-9]+)(?P<downstream>\+?)|"
        r"dag:(?P<dag>[a-zA-Z0-9][-_a-zA-Z0-9]+)|"
        r"tag:(?P<tag>[a-zA-Z0-9][-_a-zA-Z0-9]+)"
        r")$"
    )
)


class DagQueryError(Exception):
    pass


def _get_query_component(tasks, query):
    tasks = {k: {"dag": v["dag"], "tags": v.get("tags")} for k, v in tasks.items()}
    match = RE_TASK_QUERY.match(query)
    if match is None:
        raise DagQueryError(f'Incorrect task query syntax "{query}"')
    else:
        match_components = match.groupdict()

        if match_components.get("tag") is not None:
            tag = match_components["tag"]
            relevant_tasks = {k: v for k, v in tasks.items() if tag in v.get("tags")}
            if len(relevant_tasks) == 0:
                raise DagQueryError(f'Undefined tag "{tag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("dag") is not None:
            dag = match_components["dag"]
            relevant_tasks = {k: v for k, v in tasks.items() if dag == v.get("dag")}
            if len(relevant_tasks) == 0:
                raise DagQueryError(f'Undefined dag "{dag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("task") is not None:
            task = match_components["task"]
            if task not in tasks:
                raise DagQueryError(f'Undefined task "{task}"')
            return [
                {
                    "task": task,
                    "upstream": match_components.get("upstream", "") == "+",
                    "downstream": match_components.get("downstream", "") == "+",
                }
            ]


def get_query(tasks, include=list(), exclude=list()):
    overlap = set(include).intersection(set(exclude))
    if len(overlap) > 0:
        overlap = ", ".join(overlap)
        raise DagQueryError(f'Overlap between include and exclude for "{overlap}"')

    output = [
        dict(comp, operation=operation)
        for operation, components in (("include", include), ("exclude", exclude))
        for q in components
        for comp in _get_query_component(tasks, q)
    ]

    # simplify the queries by unifying upstream/downstream
    include = dict()
    exclude = dict()
    for operand in output:
        operation_dict = include if operand["operation"] == "include" else exclude
        task = operand["task"]
        if task in operation_dict:
            for flag in ("upstream", "downstream"):
                operation_dict[task][flag] = operation_dict[task][flag] or operand[flag]
        else:
            operation_dict[task] = {
                "upstream": operand["upstream"],
                "downstream": operand["downstream"],
            }

    return [
        dict(flags, task=task, operation=operation)
        for operation, operands in (("include", include), ("exclude", exclude))
        for task, flags in operands.items()
    ]


def get_presets(global_presets, dags):
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
            [presets_info[p] for p in dag_upstream(presets_dag, name)]
            + [presets_info[name]]
        )
        for name in topological_sort(presets_dag)
    }

    return presets


def get_task_dict(task, task_name, dag_name, presets):
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
    presets = get_presets(global_presets, dags)

    return {
        task_name: get_task_dict(task, task_name, dag_name, presets)
        for dag_name, dag in dags.items()
        for task_name, task in dag.tasks.items()
    }


#     dag_presets_dag = {
#         k: [v] if v is not None else []
#         for dag_name, dag in dags.items()
#         for k, v in {
#             f"{name}": preset.get("preset") for name, preset in dag.presets.items()
#         }.items()
#     }
#
#     for preset in dags.presets.values():
#         if "preset" in preset:
#             if preset["preset"] in dag_presets:
#                 merge_dicts()
#
#     tasks = {
#         task_name: get_task(task_def, dag_name, dags.presets, global_presets)
#         for dag_name, dag_def in dags.items()
#         for task_name, task_def in dag_def.tasks.items()
#     }
#
#     for task in tasks.values():
#         task.set_parents(tasks)
#
#     return tasks


class TaskWrapper:
    name = None
    dag = None
    tags = list()
    parents = list()
    executor = None
    in_query = True

    # _parent_names = list()
    # _type = None

    # TODO file line numbers

    def __init__(self, task_info):
        self.name = task_info["name"]
        self._parent_names = task_info.get("parents", list())
        self._type = task_info["type"]
        self.tags = task_info.get("tags", list())
        self._info = task_info

    def set_parents(self, all_tasks):
        self.parents = [all_tasks[p] for p in self._parent_names]

    def run(self):
        UI().info(f"Running {self.name}...")


class SaynApp:
    run_id: UUID = uuid4()
    run_start_ts = datetime.now()

    tasks = dict()
    dag = dict()

    # task_query = dict()
    # warehouse = None
    # connectors = dict()
    # progress_handlers = list()

    def set_tasks(self, tasks):
        self.tasks = tasks
        self.dag = {
            task.name: [p.name for p in task.parents] for task in self.tasks.values()
        }
