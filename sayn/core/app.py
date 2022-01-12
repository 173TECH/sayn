from datetime import datetime, date, timedelta
from enum import Enum
from itertools import groupby
from pathlib import Path
import shutil
from uuid import UUID, uuid4
import sys
from typing import List, Optional

from ..tasks.task_wrapper import TaskWrapper
from ..utils.dag import query as dag_query, topological_sort
from .settings import get_connections, get_settings
from .errors import Err, Exc, Ok, Result, SaynError
from ..logging import EventTracker
from ..database import Database

from ..core.project import read_project, read_groups, get_tasks_dict
from ..core.settings import read_settings
from ..logging import ConsoleLogger
from ..utils.python_loader import PythonLoader
from ..utils.task_query import get_query
from ..utils.compiler import Compiler

from ..tasks.task import FailedTask
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


run_id = uuid4()


class Command(Enum):
    UNDEFINED = "undefined"
    COMPILE = "compile"
    RUN = "run"
    TEST = "test"


class RunArguments:
    class Folders:
        python: str = "python"
        sql: str = "sql"
        compile: str = "compile"
        logs: str = "logs"
        tests: str = "tests"

    folders: Folders
    full_load: bool = False
    start_dt: date = date.today() - timedelta(days=1)
    end_dt: date = date.today() - timedelta(days=1)
    debug: bool = False
    profile: Optional[str] = None
    command: Command = Command.UNDEFINED

    include: List
    exclude: List

    def __init__(self):
        self.folders = self.Folders()
        self.include = list()
        self.exclude = list()


class App:
    def __init__(self):
        self.project_root = Path(".")

        self.run_id: UUID = run_id
        self.app_start_ts = datetime.now()

        self.run_arguments = RunArguments()

        self.tracker = EventTracker(self.run_id)
        self.tracker.register_logger(ConsoleLogger(True))

        self.project_parameters = dict()
        self.prod_project_parameters = dict()
        self.credentials = dict()
        self.default_db = None

        self.tasks = dict()
        self.dag = dict()
        self.tests = dict()

        self.task_query = list()
        self.tasks_to_run = dict()

        self.connections = dict()

        self.python_loader = PythonLoader()

    def start_app(self):
        self.tracker.report_event(
            context="app",
            event="start_app",
            debug=self.run_arguments.debug,
            full_load=self.run_arguments.full_load,
            start_dt=self.run_arguments.start_dt,
            end_dt=self.run_arguments.end_dt,
            profile=self.run_arguments.profile,
        )
        self.cleanup_compilation()

        # SETUP THE APP: read project config and settings, interpret cli arguments and setup the dag
        self.tracker.start_stage("config")

        # Set python environment
        if Path(self.run_arguments.folders.python).is_dir():
            self.check_abort(
                self.python_loader.register_module(
                    "python_tasks", self.run_arguments.folders.python
                )
            )

        # Read the project configuration
        try:
            project = read_project(self.project_root)
        except SaynError as exc:
            self.finish_app(error=Exc(exc))

        try:
            file_groups = read_groups(self.project_root)
        except SaynError as exc:
            self.finish_app(error=Exc(exc))

        self.set_project(project, file_groups)

        # We need the settings before we can process the tasks
        settings = self.check_abort(read_settings())
        self.check_abort(self.set_settings(settings))

        # Create the jinja compiler
        self.compiler = Compiler(self.project_parameters, self.prod_project_parameters)

        # Set tasks and dag from it
        tasks_dict = self.check_abort(
            get_tasks_dict(
                self.presets,
                self.file_groups,
                self.autogroups,
                self.run_arguments.folders.sql,
                self.compiler,
            )
        )

        # Set the tasks for the project and call their config method
        self.check_abort(self.set_tasks(tasks_dict))

        print("stop here")
        import IPython

        IPython.embed()
        # Apply the task query
        self.task_query = self.check_abort(
            get_query(
                tasks_dict,
                include=self.run_arguments.include,
                exclude=self.run_arguments.exclude,
            )
        )

        # TODO filter

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in self.tasks.items()}
        )

    def set_project(self, project, file_groups):
        self.prod_project_parameters.update(project.parameters or dict())
        self.project_parameters.update(project.parameters or dict())

        self.prod_stringify = {
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
        self.stringify = {
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

        self.presets = project.presets or dict()

        # Validate groups
        collision = set(file_groups.keys()).intersection(set(project.autogroups.keys()))
        if len(collision) > 0:
            if len(collision) == 1:
                error_message = (
                    f'The group "{", ".join(collision)}" is defined both in '
                    '"project.yaml" and as a file in the "tasks" folder: '
                )
            else:
                error_message = (
                    f'Some groups ({", ".join(collision)}) are defined both in '
                    '"project.yaml" and as files in the "tasks" folder: '
                )
            self.finish_app(
                error=Err(
                    "dag",
                    "duplicate_groups",
                    error_message=error_message,
                    groups=list(collision),
                )
            )
        self.autogroups = project.autogroups
        self.file_groups = file_groups

    def set_settings(self, settings):
        settings_dict = get_settings(
            settings["yaml"], settings["env"], self.run_arguments.profile
        )
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

        self.stringify.update(stringify)

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
        result = get_connections(self.credentials, self.stringify, self.prod_stringify)
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

    def set_tasks(self, tasks):
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
                self.stringify,
                self.run_arguments,
                self.compiler.get_task_compiler(task_objects[task_name]),
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

        if self.run_arguments.command == Command.TEST:
            # TODO move this logic to the task config
            self.dag = {
                task.name: []
                for task in tasks.values()
                if "columns" in task.keys() or "test" in task["type"]
            }
        else:
            self.dag = {
                task.name: [p.name for p in task.parents]
                for task in task_objects.values()
                # TODO - fix for tests
                # if "test" not in task["type"]
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
            task_tracker._task_order = task_order + 1
            #  TODO - review from test
            # for task_name, task in self._tasks_dict.items():
            #     if self.run_arguments["command"] == "test":
            #         parents = []
            #     else:
            #         parents = [self.tasks[p] for p in task.get("parents", list())]

            #     task_tracker = self.tracker.get_task_tracker(task_name)
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

    def check_abort(self, result):
        """Interpret the result of setup opreations returning the value if `result.is_ok`.

        Setup errors from the cli result in execution abort.

        Args:
          result (sayn.errors.Result): The result of a setup operation
        """
        if result is None or not isinstance(result, Result):
            self.finish_app(error=Err("app_setup", "unhandled_error", result=result))
        elif result.is_err:
            self.finish_app(result)
        else:
            return result.value

    def run(self):
        self.execute_dag()

    def compile(self):
        self.execute_dag()

    def test(self):
        self.execute_dag()

    def execute_dag(self):
        # Execution of relevant tasks
        tasks_in_query = {k: v for k, v in self.tasks.items() if v.in_query}
        self.tracker.start_stage(
            self.run_arguments.command.value, tasks=list(tasks_in_query.keys())
        )

        for task in self.tasks.values():
            if not task.inquery:
                continue

            # We force the run/compile so that the skipped status can be calculated,
            # but we only report if the task is in the query
            # if task.in_query:
            task.tracker._report_event("start_stage")
            start_ts = datetime.now()

            if self.run_arguments.command == Command.RUN:
                result = task.run()
            elif self.run_arguments.command == Command.COMPILE:
                result = task.compile()
            elif self.run_arguments.command == Command.TEST:
                result = task.test()
            else:
                self.finish_app(error=Err("cli", "wrong_command"))

            if task.in_query:
                task.tracker._report_event(
                    "finish_stage", duration=datetime.now() - start_ts, result=result
                )

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in tasks_in_query.items()}
        )

        self.finish_app()

    def finish_app(self, error=None):
        duration = datetime.now() - self.app_start_ts
        if error is None:
            self.tracker.report_event(
                event="finish_app",
                duration=duration,
                tasks={k: v.status for k, v in self.tasks.items()},
            )
            sys.exit(0)
        else:
            self.tracker.report_event(
                event="finish_app",
                duration=duration,
                error=error,
            )
            sys.exit(-1)

    def cleanup_compilation(self):
        folder = self.run_arguments.folders.compile
        compile_path = Path(folder)
        if compile_path.exists():
            if compile_path.is_dir():
                shutil.rmtree(compile_path.absolute())
            else:
                compile_path.unlink()

        compile_path.mkdir()
