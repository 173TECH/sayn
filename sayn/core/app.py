from datetime import datetime
from uuid import UUID, uuid4

from .config import read_project, read_dags, read_settings, get_tasks_dict
from ..tasks import Task
from ..utils.ui import UI
from ..utils.dag import query as dag_query, topological_sort
from ..utils.task_query import get_query


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
