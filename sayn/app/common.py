from datetime import datetime
from pathlib import Path
import re
from uuid import UUID, uuid4

import yaml


from ..utils.ui import UI

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
    tasks = {k: {"dag": v.dag, "tags": v.tags} for k, v in tasks.items()}
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


def read_project():
    project_yaml = yaml.safe_load(Path("project.yaml").read_text())
    project_yaml["dags"] = {
        dag: yaml.safe_load(Path(f"dags/{dag}.yaml").read_text())
        for dag in project_yaml["dags"]
    }
    return project_yaml


def get_tasks(project):
    tasks = {
        task_name: TaskWrapper(dict(task_def, name=task_name, dag=dag_name))
        for dag_name, dag_def in project["dags"].items()
        for task_name, task_def in dag_def["tasks"].items()
    }

    for task in tasks.values():
        task.set_parents(tasks)

    return tasks


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
