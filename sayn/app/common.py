from datetime import datetime
import re
from uuid import UUID, uuid4

from ..utils.ui import UI
from ..utils.dag import query as dag_query
from ..utils.dag import topological_sort
from ..tasks import Task
from .config import read_project, read_dags, read_settings, get_tasks_dict

#####################################
# Task query interpretation functions
#####################################

RE_TASK_QUERY = re.compile(
    (
        r"^("
        r"(?!(dag:|tag:))(?P<upstream>\+?)(?P<task>[a-zA-Z0-9][-_a-zA-Z0-9]+)(?P<downstream>\+?)|"
        r"dag:(?P<dag>[a-zA-Z0-9][-_a-zA-Z0-9]+)|"
        r"tag:(?P<tag>[a-zA-Z0-9][-_a-zA-Z0-9]+)"
        r")$"
    )
)


class TaskQueryError(Exception):
    pass


def _get_query_component(tasks, query):
    tasks = {k: {"dag": v["dag"], "tags": v.get("tags")} for k, v in tasks.items()}
    match = RE_TASK_QUERY.match(query)
    if match is None:
        raise TaskQueryError(f'Incorrect task query syntax "{query}"')
    else:
        match_components = match.groupdict()

        if match_components.get("tag") is not None:
            tag = match_components["tag"]
            relevant_tasks = {k: v for k, v in tasks.items() if tag in v.get("tags")}
            if len(relevant_tasks) == 0:
                raise TaskQueryError(f'Undefined tag "{tag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("dag") is not None:
            dag = match_components["dag"]
            relevant_tasks = {k: v for k, v in tasks.items() if dag == v.get("dag")}
            if len(relevant_tasks) == 0:
                raise TaskQueryError(f'Undefined dag "{dag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("task") is not None:
            task = match_components["task"]
            if task not in tasks:
                raise TaskQueryError(f'Undefined task "{task}"')
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
        raise TaskQueryError(f'Overlap between include and exclude for "{overlap}"')

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


class SaynApp:
    run_id: UUID = uuid4()
    run_start_ts = datetime.now()
    ui = None

    tasks = dict()
    dag = dict()

    task_query = list()

    connections = dict()
    default_db = None

    def __init__(self, debug):
        self.ui = UI(run_id=self.run_id, debug=debug)
        self.ui._set_config(stage_name="setup")

    def setup(
        self,
        include=list(),
        exclude=list(),
        profile=None,
        full_load=False,
        start_dt=None,
        end_dt=None,
    ):
        # Read the project configuration
        self.set_project(read_project())
        self.set_dags(read_dags(self.project.dags))
        self.set_settings(read_settings())

        # Set tasks and dag from it
        tasks_dict = get_tasks_dict(self.project.presets, self.dags)
        self.task_query = get_query(tasks_dict, include=include, exclude=exclude)
        self.set_tasks(tasks_dict)

        # Set settings and connections

    def set_project(self, project):
        self.project = project

    def set_dags(self, dags):
        self.dags = dags

    def set_settings(self, settings):
        self.settings = settings

    def set_tasks(self, tasks):
        self.dag = {
            task["name"]: [p for p in task.get("parents", list())]
            for task in tasks.values()
        }

        self._tasks_dict = {
            task_name: tasks[task_name] for task_name in topological_sort(self.dag)
        }

        tasks_in_query = dag_query(self.dag, self.task_query)

        for task_name, task in self._tasks_dict.items():
            self.tasks[task_name] = Task(
                task,
                [self.tasks[p] for p in task.get("parents", list())],
                task_name in tasks_in_query,
                self.project.parameters,
                list(),
                self.ui,
            )

    def run(self):
        for task_name, task in self.tasks.items():
            task.run()

    def compile(self):
        for task_name, task in self.tasks.items():
            task.compile()
