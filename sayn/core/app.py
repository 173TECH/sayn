from datetime import datetime, date, timedelta
from enum import Enum
from itertools import groupby
from pathlib import Path
import shutil
from uuid import UUID, uuid4
import sys
from typing import Optional, Set

from ..tasks.task_wrapper import TaskWrapper
from ..utils.dag import query as dag_query, topological_sort
from .settings import get_connections, get_settings
from .errors import Err, Exc, Ok, Result, SaynError
from ..logging import EventTracker
from ..database import Database

from ..core.project import read_project, read_groups, get_tasks_dict
from ..core.settings import read_settings
from ..database.unknown import UnknownDb
from ..database.objects import DbObjectCompiler
from ..logging import ConsoleLogger
from ..utils.python_loader import PythonLoader
from ..utils.task_query import get_query
from ..utils.compiler import Compiler

from ..tasks.task import TaskStatus
from ..tasks.dummy import DummyTask
from ..tasks.sql import SqlTask
from ..tasks.autosql import AutoSqlTask
from ..tasks.copy import CopyTask
from ..tasks.test import TestTask

_creators = {
    "dummy": DummyTask,
    "sql": SqlTask,
    "autosql": AutoSqlTask,
    "copy": CopyTask,
    "test": TestTask,
}


run_id = uuid4()


class Command(Enum):
    UNDEFINED = "undefined"
    COMPILE = "compile"
    RUN = "run"
    TEST = "test"


class RunArguments:
    class Folders:
        python: str = str(Path("python"))
        sql: str = str(Path("sql"))
        compile: str = str(Path("compile"))
        logs: str = str(Path("logs"))
        tests: str = str(Path("sql"))

    folders: Folders
    full_load: bool = False
    start_dt: date = date.today() - timedelta(days=1)
    end_dt: date = date.today() - timedelta(days=1)
    dates_specified: bool = False
    debug: bool = False
    profile: Optional[str] = None
    command: Command = Command.UNDEFINED
    upstream_prod: bool = False
    is_prod: bool = False

    include: Set[str]
    exclude: Set[str]

    def __init__(self):
        self.folders = self.Folders()
        self.include = set()
        self.exclude = set()

    def update(self, **kwargs):
        if "command" in kwargs:
            if isinstance(kwargs["command"], str):
                if kwargs["command"] == "compile":
                    self.command = Command.COMPILE
                elif kwargs["command"] == "run":
                    self.command = Command.RUN
                elif kwargs["command"] == "test":
                    self.command = Command.TEST
            else:
                self.command = kwargs["command"]

        if "debug" in kwargs:
            self.debug = kwargs["debug"]


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

        # Set tasks and dag from it
        tasks_dict = self.check_abort(
            get_tasks_dict(
                self.presets,
                self.file_groups,
                self.autogroups,
                self.run_arguments.folders.sql,
                self.compiler,
                self.python_loader,
            )
        )

        # Set the tasks for the project and call their config method

        if self.run_arguments.command != Command.TEST:
            tasks_dict = {k: v for k, v in tasks_dict.items() if v["type"] != "test"}

        self.check_abort(self.set_tasks(tasks_dict))

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in self.tasks.items()},
            test=True if self.run_arguments.command == Command.TEST else False,
        )

        # Setup stage
        self.tracker.start_stage("setup")

        self.check_abort(self.setup_execution())

        self.tracker.finish_current_stage(
            tasks={k: v.status for k, v in self.tasks.items() if v.in_query},
            test=True if self.run_arguments.command == Command.TEST else False,
        )

    def set_project(self, project, file_groups):
        self.prod_project_parameters.update(project.parameters or dict())
        self.project_parameters.update(project.parameters or dict())

        # Temporarily store the raw stringify strings
        self.input_prod_stringify = {
            "database_prefix": None,  # project.database_prefix,
            "database_suffix": None,  # project.database_suffix,
            "database_override": None,  # project.database_override,
            "schema_prefix": project.schema_prefix,
            "schema_suffix": project.schema_suffix,
            "schema_override": project.schema_override,
            "table_prefix": project.table_prefix,
            "table_suffix": project.table_suffix,
            "table_override": project.table_override,
        }
        self.input_stringify = {
            "database_prefix": None,  # project.database_prefix,
            "database_suffix": None,  # project.database_suffix,
            "database_override": None,  # project.database_override,
            "schema_prefix": project.schema_prefix,
            "schema_suffix": project.schema_suffix,
            "schema_override": project.schema_override,
            "table_prefix": project.table_prefix,
            "table_suffix": project.table_suffix,
            "table_override": project.table_override,
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
        self.from_prod = settings_dict["from_prod"]

        if len(parameters) == 0 and len(stringify) == 0:
            self.run_arguments.is_prod = True

        # Validate the given parameters
        error_items = set(parameters.keys()) - set(self.project_parameters.keys())
        if error_items:
            return Err(
                "app",
                "wrong_parameters",
                parameters=error_items,
            )

        self.project_parameters.update(parameters)

        # With the parameters in place we can build our jinja compiler
        self.compiler = Compiler(
            self.run_arguments, self.project_parameters, self.prod_project_parameters
        )

        # Validate credentials
        error_items = set(credentials.keys()) - set(self.credentials.keys())
        if error_items:
            return Err("app", "wrong_credentials", credentials=error_items)

        error_items = [n for n, v in credentials.items() if "type" not in v]
        if error_items:
            return Err("app", "missing_credential_type", credentials=error_items)

        self.credentials.update(credentials)

        # Create connections
        result = get_connections(
            self.credentials,
        )
        if result.is_err:
            return result
        else:
            self.connections = result.value

        # Object compilation objects
        self.input_stringify.update(stringify)
        self.db_object_compiler = DbObjectCompiler(
            self.connections,
            self.default_db,
            self.input_stringify,
            self.input_prod_stringify,
            self.from_prod,
        )

        # Check the default_run setting and update run arguments
        self.run_arguments.include.update(settings_dict["default_run"]["include"])
        self.run_arguments.exclude.update(settings_dict["default_run"]["exclude"])
        if settings_dict["default_run"]["upstream_prod"] is not None:
            self.run_arguments.upstream_prod = settings_dict["default_run"][
                "upstream_prod"
            ]

        return Ok()

    def get_task_class(self, task_type, config):
        if task_type == "python_module":
            return Ok(config.pop("task_class"))
        elif task_type == "python":
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
        failed_tasks = list()
        task_objects = dict()

        if len(tasks) == 0:
            self.finish_app(Err("dag", "empty_dag"))

        for task_name, task in tasks.items():
            task_tracker = self.tracker.get_task_tracker(task_name)
            task_tracker._report_event("start_stage")
            start_ts = datetime.now()

            result = self.get_task_class(task["type"], task)
            if result.is_err:
                task_class = None
                result_error = result
                failed_tasks.append(task_name)
            else:
                task_class = result.value
                result_error = None

            task_objects[task_name] = TaskWrapper(
                task["group"],
                task_name,
                task["type"],
                task.get("on_fail"),
                task.get("parents"),
                task.get("sources"),
                task.get("outputs"),
                task.get("tags"),
                task_tracker,
                task_class,
                self.connections,
                self.default_db,
                self.run_arguments,
                self.compiler,
                self.db_object_compiler,
            )

            if task_class is None:
                task_tracker._report_event(
                    "finish_stage",
                    duration=datetime.now() - start_ts,
                    result=result_error,
                )

            else:
                result = task_objects[task_name].config(
                    task,
                    self.project_parameters,
                    task.get("parameters"),
                )

                if result.is_err:
                    failed_tasks.append(task_name)

                task_tracker._report_event(
                    "finish_stage", duration=datetime.now() - start_ts, result=result
                )

        if len(failed_tasks) > 0:
            # If any tasks fail to do config, we can't ensure the DAG is correct, so we abort
            self.tasks = task_objects
            self.tracker.finish_current_stage(
                tasks={k: v.status for k, v in self.tasks.items()}
            )

            self.finish_app()

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
            result = task.set_parents(task_objects, output_to_task)
            if result.is_err:
                return result

        self.dag = {
            task.name: [p.name for p in task.parents] for task in task_objects.values()
        }

        topo_sort = topological_sort(self.dag)
        if topo_sort.is_err:
            return topo_sort
        else:
            topo_sort = topo_sort.value

        self.tasks = {task_name: task_objects[task_name] for task_name in topo_sort}

        return Ok()

    def setup_execution(self):
        # Apply the task query
        tasks_dict = {
            name: {"group": task.group, "tags": list(task.tags)}
            for name, task in self.tasks.items()
        }
        self.task_query = self.check_abort(
            get_query(
                tasks_dict,
                include=self.run_arguments.include,
                exclude=self.run_arguments.exclude,
            )
        )

        result = dag_query(self.dag, self.task_query)
        if result.is_err:
            return result
        else:
            tasks_in_query = result.value

        if self.run_arguments.command == Command.TEST:
            tasks_in_query = [t for t in tasks_in_query if self.tasks[t].has_tests()]

        # Introspection
        #########

        # Create the list of objects to be used in this execution
        exec_outputs = set()
        exec_sources = set()
        exec_connections = set()
        for task_name in tasks_in_query:
            for output in self.tasks[task_name].outputs:
                exec_outputs.add(output)
                # if output.connection not in objects_used:
                #     objects_used[output.connection] = set()
                # objects_used[output.connection].add(output)

            for source in self.tasks[task_name].sources:
                exec_sources.add(source)
                # if source.connection not in objects_used:
                #     objects_used[source.connection] = set()
                # objects_used[source.connection].add(source)

            for connection in self.tasks[task_name].used_connections:
                exec_connections.add(connection)

        # Now that we have done the config for all tasks and we know which
        # connections are required, check that we have them all
        connections_setup = {
            n for n, v in self.connections.items() if not isinstance(v, UnknownDb)
        }
        error_items = exec_connections - connections_setup
        if error_items:
            return Err("app", "missing_credentials", credentials=error_items)

        if self.run_arguments.upstream_prod:
            sources_from_prod = {s for s in exec_sources if s not in exec_outputs}
        else:
            sources_from_prod = set()

        self.db_object_compiler.set_sources_from_prod(sources_from_prod)
        sources_from_prod = {
            s for s in exec_sources if self.db_object_compiler.is_from_prod(s)
        }

        # Get the objects to introspect
        # We create new DbObjects simply because it's simpler to convert to the correct
        # specification depending on whether it's from_prod or not, but these new objects
        # are not meant to be used anywhere else in the code
        to_introspect = {self.db_object_compiler.src_obj(s) for s in exec_sources}
        to_introspect.update({self.db_object_compiler.out_obj(o) for o in exec_outputs})

        # Reshape the list into dictionaries of connection > database > schema > set of objects
        to_introspect = {
            conn: {
                db
                or "": {
                    sch or "": {v.table for v in ggg}
                    for sch, ggg in groupby(gg, lambda x: x.schema)
                }
                for db, gg in groupby(g, lambda x: x.database)
            }
            for conn, g in groupby(
                sorted(
                    to_introspect,
                    key=lambda x: (x.connection_name, x.database, x.schema, x.table),
                ),
                key=lambda x: x.connection_name,
            )
        }

        for connection_name in exec_connections:
            db = self.connections[connection_name]
            if isinstance(db, Database):
                try:
                    db._activate_connection()  # This call creates the engine and tests the connection
                except Exception as exc:
                    return Exc(exc, where="create_connection")

                try:
                    db._introspect(to_introspect[connection_name])
                except Exception as exc:
                    return Exc(exc, where="introspection")

        self.tracker.set_tasks(tasks_in_query)

        for task_order, task_name in enumerate(tasks_in_query):
            task = self.tasks[task_name]
            task.tracker._task_order = task_order + 1
            if task.status != TaskStatus.READY_FOR_SETUP:
                continue
            #  TODO - review from test
            # for task_name, task in self._tasks_dict.items():
            #     if self.run_arguments["command"] == "test":
            #         parents = []
            #     else:
            #         parents = [self.tasks[p] for p in task.get("parents", list())]

            #     task_tracker = self.tracker.get_task_tracker(task_name)

            start_ts = datetime.now()

            task.tracker._report_event("start_stage")

            result = task.setup(task_name in tasks_in_query, sources_from_prod)

            task.tracker._report_event(
                "finish_stage", duration=datetime.now() - start_ts, result=result
            )

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
            if not task.in_query:
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
            tasks={k: v.status for k, v in tasks_in_query.items()},
            test=True if self.run_arguments.command == Command.TEST else False,
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
