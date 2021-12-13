from datetime import datetime, date, timedelta
from itertools import groupby
from uuid import UUID, uuid4

from ..tasks.task_wrapper import TaskWrapper
from ..utils.dag import query as dag_query, topological_sort
from .config import get_connections
from .errors import Err, Ok
from ..logging import EventTracker
from ..database import Database

from ..tasks import FailedTask
from ..tasks.dummy import DummyTask
from ..tasks.sql import SqlTask
from ..tasks.autosql import AutoSqlTask
from ..tasks.copy import CopyTask

_creators = {
    "dummy": DummyTask,
    "sql": SqlTask,
    "autosql": AutoSqlTask,
    "copy": CopyTask,
}

_excluded_properties = (
    "name",
    "type",
    "tags",
    "group",
    "parents",
    "sources",
    "outputs",
    "parameters",
    "class",
    "preset",
    "on_fail",
)


run_id = uuid4()


class App:
    run_id: UUID = run_id
    app_start_ts = datetime.now()
    tracker = None  # TODO create a default event tracker EventTracker(run_id)

    run_arguments = {
        "folders": {
            "python": "python",
            "sql": "sql",
            "compile": "compile",
            "logs": "logs",
        },
        "full_load": False,
        "start_dt": date.today() - timedelta(days=1),
        "end_dt": date.today() - timedelta(days=1),
        "debug": False,
        "profile": None,
    }

    project_parameters = dict()
    credentials = dict()
    default_db = None

    tasks = dict()
    dag = dict()

    task_query = list()
    tasks_to_run = dict()

    connections = dict()

    python_loader = None

    def set_project(self, project):
        self.project_parameters.update(project.parameters or dict())
        self.stringify_production = {
            "database_prefix": project.database_prefix,
            "database_suffix": project.database_suffix,
            "database_stringify": project.database_stringify,
            "schema_prefix": project.schema_prefix,
            "schema_suffix": project.schema_suffix,
            "schema_stringify": project.schema_stringify,
            "table_prefix": project.table_prefix,
            "table_suffix": project.table_suffix,
            "table_stringify": project.table_stringify,
        }
        self.stringify_runtime = {
            "database_prefix": project.database_prefix,
            "database_suffix": project.database_suffix,
            "database_stringify": project.database_stringify,
            "schema_prefix": project.schema_prefix,
            "schema_suffix": project.schema_suffix,
            "schema_stringify": project.schema_stringify,
            "table_prefix": project.table_prefix,
            "table_suffix": project.table_suffix,
            "table_stringify": project.table_stringify,
        }
        self.credentials = {k: None for k in project.required_credentials}
        self.default_db = project.default_db

    def set_settings(self, settings):
        settings_dict = settings.get_settings(self.run_arguments["profile"])
        if settings_dict.is_err:
            return settings_dict
        else:
            settings_dict = settings_dict.value

        parameters = settings_dict["parameters"] or dict()
        credentials = settings_dict["credentials"] or dict()
        stringify = settings_dict["stringify"] or dict()

        # profile_name = self.run_arguments["profile"]

        # Get parameters and credentials from yaml
        # if settings.yaml is not None:
        #     if profile_name is not None and profile_name not in settings.yaml.profiles:
        #         return Err("app_command", "wrong_profile", profile=profile_name)

        #     profile_name = profile_name or settings.yaml.default_profile

        #     parameters = settings.yaml.profiles[profile_name].parameters or dict()

        #     credentials = {
        #         prof_name: settings.yaml.credentials[yaml_name]
        #         for prof_name, yaml_name in settings.yaml.profiles[
        #             profile_name
        #         ].credentials.items()
        #     }

        #     stringify = {
        #         k: v
        #         for k, v in settings.yaml.profiles[profile_name].stringify.items()
        #         if v is not None
        #     }

        #     self.run_arguments["profile"] = profile_name

        # # Update parameters and credentials with environment
        # if settings.environment is not None:
        #     parameters.update(settings.environment.parameters or dict())
        #     credentials.update(settings.environment.credentials or dict())
        #     stringify.update(
        #         {
        #             k: v
        #             for k, v in settings.environment.stringify.items()
        #             if v is not None
        #         }
        #     )

        # Validate the given parameters
        error_items = set(parameters.keys()) - set(self.project_parameters.keys())
        if error_items:
            return Err(
                "app",
                "wrong_parameters",
                parameters=error_items,
            )

        self.project_parameters.update(parameters)

        self.stringify_runtime.update(stringify)

        # Validate credentials
        error_items = set(credentials.keys()) - set(self.credentials.keys())
        if error_items:
            return Err("app", "wrong_credentials", credentials=error_items)

        error_items = set(self.credentials.keys()) - set(credentials.keys())
        if error_items:
            return Err("app", "missing_credentials", credentials=error_items)

        error_items = [n for n, v in credentials.items() if "type" not in v]
        if error_items:
            return Err("app", "missing_credential_type", credentials=error_items)

        self.credentials.update(credentials)

        # Create connections
        result = get_connections(self.credentials)
        if result.is_err:
            return result
        else:
            self.connections = result.value

        return Ok()

    def get_task_class(self, task_type, config):
        if task_type == "python":
            return self.python_loader.get_class("python_tasks", config.get("class"))
        elif task_type in _creators:
            return Ok(_creators[task_type])
        else:
            return Err(
                "task_type",
                "invalid_task_type_error",
                type=task_type,
                group=config["group"],
            )

    def set_tasks(self, tasks, task_query):
        self.task_query = task_query

        # We first need to do the config of tasks
        task_objects = dict()
        for task_name, task in tasks.items():
            task_tracker = self.tracker.get_task_tracker(task_name)
            task_tracker._report_event("start_stage")
            start_ts = datetime.now()

            result = self.get_task_class(task["type"], task)
            if result.is_err:
                task_class = FailedTask
            else:
                task_class = result.value

            task_objects[task_name] = TaskWrapper(
                task["group"],
                task_name,
                task["type"],
                task.get("parameters"),
                task.get("on_fail"),
                task.get("parents"),
                task.get("sources"),
                task.get("outputs"),
                task.get("tags"),
                task,
                task_tracker,
                task_class,
            )

            result = task_objects[task_name].config(
                self.connections,
                self.default_db,
                self.project_parameters,
                self.stringify_runtime,
                self.run_arguments,
            )

            task_tracker._report_event(
                "finish_stage", duration=datetime.now() - start_ts, result=result
            )

        # Now that all tasks are configured, we set the relationships so that we
        # can calculate the dag

        output_to_task = [
            (output, task_name)
            for task_name, task in task_objects.items()
            for output in task.outputs
        ]

        output_to_task = {
            k: [gg[1] for gg in g]
            for k, g in groupby(sorted(output_to_task), key=lambda x: x[0])
        }
        for task_name, task in task_objects.items():
            task.set_parents(task_objects, output_to_task)

        self.dag = {
            task.name: [p.name for p in task.parents] for task in task_objects.values()
        }

        topo_sort = topological_sort(self.dag)
        if topo_sort.is_err:
            return topo_sort
        else:
            topo_sort = topo_sort.value

        self.tasks = {task_name: task_objects[task_name] for task_name in topo_sort}

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in self.tasks.items()}
        )

        self.tracker.start_stage("setup")

        result = dag_query(self.dag, self.task_query)
        if result.is_err:
            return result
        else:
            tasks_in_query = result.value
        self.tracker.set_tasks(tasks_in_query)

        for task_order, task_info in enumerate(self.tasks.items()):
            task_name, task = task_info
            task_tracker = task.tracker
            task_tracker._task_order = task_order
            if task_name in tasks_in_query:
                task_tracker._report_event("start_stage")
            start_ts = datetime.now()

            result = self.tasks[task_name].setup(task_name in tasks_in_query)

            if task_name in tasks_in_query:
                task_tracker._report_event(
                    "finish_stage", duration=datetime.now() - start_ts, result=result
                )

        for db in self.connections.values():
            if isinstance(db, Database):
                db._introspect()

        return Ok()

    # Commands

    def run(self):
        self.execute_dag("run")

    def compile(self):
        self.execute_dag("compile")

    def execute_dag(self, command):
        self.run_arguments["command"] = command
        # Execution of relevant tasks
        tasks_in_query = {k: v for k, v in self.tasks.items() if v.in_query}
        self.tracker.start_stage(command, tasks=list(tasks_in_query.keys()))

        for task_name, task in self.tasks.items():
            # We force the run/compile so that the skipped status can be calculated,
            # but we only report if the task is in the query
            if task.in_query:
                task.tracker._report_event("start_stage")
                start_ts = datetime.now()

            if command == "run":
                result = task.run()
            else:
                result = task.compile()

            if task.in_query:
                task.tracker._report_event(
                    "finish_stage", duration=datetime.now() - start_ts, result=result
                )

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in tasks_in_query.items()}
        )

        self.finish_app()

    def start_app(self, loggers, **run_arguments):
        run_arguments["start_dt"] = run_arguments["start_dt"].date()
        run_arguments["end_dt"] = run_arguments["end_dt"].date()
        self.tracker = EventTracker(self.run_id, loggers, run_arguments=run_arguments)
        self.run_arguments.update(run_arguments)

    def finish_app(self, error=None):
        duration = datetime.now() - self.app_start_ts
        if error is None:
            self.tracker.report_event(
                event="finish_app",
                duration=duration,
                tasks={k: v.status for k, v in self.tasks.items()},
            )
        else:
            self.tracker.report_event(
                event="finish_app",
                duration=duration,
                error=error,
            )
