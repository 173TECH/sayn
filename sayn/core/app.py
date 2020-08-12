from datetime import datetime
from uuid import UUID, uuid4

from ..tasks.task_wrapper import TaskWrapper
from .logger import AppLogger
from ..utils.dag import query as dag_query, topological_sort


class App:
    run_id: UUID = uuid4()
    run_start_ts = datetime.now()
    logger = None

    tasks = dict()
    dag = dict()

    task_query = list()

    connections = dict()
    default_db = None

    def __init__(self, debug=False):
        self.logger = AppLogger(run_id=self.run_id, debug=debug)
        self.logger.set_stage("setup")

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
            self.logger.set_current_task(task_name)
            self.tasks[task_name] = TaskWrapper(
                task,
                [self.tasks[p] for p in task.get("parents", list())],
                task_name in tasks_in_query,
                self.logger.get_task_logger(task_name),
                self.project.parameters,
                self.connections,
                self.default_db,
            )

        self.logger.set_tasks(tasks_in_query)

    def run(self):
        self._execute_dag("run")

    def compile(self):
        self._execute_dag("compile")

    def _execute_dag(self, command):
        self.logger.set_stage(command)
        for task_name, task in self.tasks.items():
            self.logger.set_current_task(task_name)
            if command == "run":
                task.run()
            else:
                task.compile()
