from datetime import datetime, date, timedelta
from uuid import UUID, uuid4

from ..tasks.task_wrapper import TaskWrapper
from ..utils.dag import query as dag_query, topological_sort
from .config import get_connections
from .errors import Err, Ok
from ..logging import EventTracker
from ..database import Database

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

    def set_tasks(self, tasks, task_query):
        self.task_query = task_query

        self.dag = {
            task["name"]: [p for p in task.get("parents", list())]
            for task in tasks.values()
        }

        topo_sort = topological_sort(self.dag)
        if topo_sort.is_err:
            return topo_sort

        self._tasks_dict = {
            task_name: tasks[task_name] for task_name in topo_sort.value
        }

        result = dag_query(self.dag, self.task_query)
        if result.is_err:
            return result
        else:
            tasks_in_query = result.value
        self.tracker.set_tasks(tasks_in_query)

        for task_name, task in self._tasks_dict.items():
            task_tracker = self.tracker.get_task_tracker(task_name)
            if task_name in tasks_in_query:
                task_tracker._report_event("start_stage")
            start_ts = datetime.now()

            self.tasks[task_name] = TaskWrapper()
            result = self.tasks[task_name].setup(
                task,
                [self.tasks[p] for p in task.get("parents", list())],
                task_name in tasks_in_query,
                task_tracker,
                self.connections,
                self.default_db,
                self.project_parameters,
                self.stringify_runtime,
                self.run_arguments,
                self.python_loader,
            )

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
