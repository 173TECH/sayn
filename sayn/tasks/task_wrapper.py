from copy import deepcopy
from typing import Any, Dict, Optional, Set

from ..database.unknown import UnknownDb

from ..core.errors import Err, Exc, Ok, Result
from ..utils.misc import map_nested

from .task import Task, TaskStatus

# Properties from a task dictionary that won't be part of the task
# configuration as they are standard SAYN task properties
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


class TaskWrapper:
    """Task wrapper managing the execution of tasks.

    This class wraps a task runner (sayn.tasks.Task) and manages the lifetime of said task.
    In a SAYN execution there will be 1 TaskWrapper per task in the project, whether the task
    is to be executed or ignored based on the task query (`-t` and `-x` arguments to `sayn`).

    Properties:
      name (str): the name of the task
      group (str): the name of the task group file where the task was defined
      tags (List[str]): list of tags declared for the task
      parents (List[sayn.task_wrapper.TaskWrapper]): the list of parents to the current task
      project_parameters (Dict[str, Any]): parameters defined at project level
      task_parameters (Dict[str, Any]): parameters defined at task level
      in_query (bool): whether the task is selected for execution based on the task query
      runner (Task): the object that will do the actual work
      status (TaskStatus): the current status of the task
    """

    name: str
    group: str
    tags: Set[str]
    parent_names: Set[str]
    sources_yaml: Set[Any]
    outputs_yaml: Set[Any]
    used_connections: Set[str]
    project_parameters: Dict[str, Any]
    task_parameters: Dict[str, Any]
    in_query: bool = False
    runner: Optional[Task]
    status: TaskStatus = TaskStatus.UNKNOWN

    def __init__(
        self,
        group: str,
        name: str,
        task_type: str,
        on_fail: str,
        parent_names: Set[str],
        sources: Set[str],
        outputs: Set[str],
        tags: Set[str],
        tracker,
        task_class,
        connections,
        default_db,
        run_arguments,
        compiler,
        db_object_compiler,
    ):
        self.tags = set(tags or set())
        self.parent_names = set(parent_names or set())
        self.parents = list()
        self.sources_yaml = set(sources or set())
        self.sources = set()
        self.outputs_yaml = set(outputs or set())
        self.outputs = set()
        self.sources_from_prod = set()
        self.used_connections = set()
        self.tracker = tracker
        self.runner = None

        self.name = name
        self.group = group
        self.task_type = task_type
        self.on_fail = on_fail or "skip"

        self.default_db = default_db
        self.connections = dict(connections)
        self.db_object_compiler = db_object_compiler

        self.task_class = task_class

        if self.task_class is None:
            self.status = TaskStatus.FAILED
        else:
            self.status = TaskStatus.CONFIGURING

            self.compiler = compiler.get_task_compiler(group=self.group, name=self.name)

            self.run_arguments = {
                "debug": run_arguments.debug,
                "full_load": run_arguments.full_load,
                "start_dt": run_arguments.start_dt,
                "end_dt": run_arguments.end_dt,
                "dates_specified": run_arguments.dates_specified,
                "command": run_arguments.command.value,
                "is_prod": run_arguments.is_prod,
                "folders": {
                    "python": run_arguments.folders.python,
                    "sql": run_arguments.folders.sql,
                    "compile": run_arguments.folders.compile,
                    "logs": run_arguments.folders.logs,
                    "tests": run_arguments.folders.tests,
                },
            }

    def config(
        self,
        task_config: Dict[str, Any],
        project_parameters: Dict[str, Any],
        task_parameters: Dict[str, Any],
    ):
        # Check the parents are in a good state
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        try:
            self.set_parameters(project_parameters, task_parameters)
        except Exception as exc:
            self.status = TaskStatus.FAILED
            return Exc(exc, where="set_task_parameters")

        # for decorator tasks
        if hasattr(self.task_class, "func_arguments"):
            for arg in self.task_class.func_arguments:
                if arg in self.connections:
                    self.used_connections.add(arg)

        try:
            runner = self.task_class(
                self.name,
                self.group,
                self.tracker,
                self.run_arguments,
                self.task_parameters,
                self.project_parameters,
                self.default_db,
                self.connections,
                self.compiler,
                self.src,
                self.out,
            )
        except Exception as exc:
            self.status = TaskStatus.FAILED
            return Exc(exc, where="create_task_runner")

        # convert sources and outputs from strings in yaml
        # Now we can compile the other properties
        runner_config = {
            k: v for k, v in task_config.items() if k not in _excluded_properties
        }

        try:
            runner_config = map_nested(
                runner_config,
                lambda x: self.compiler.compile(x) if isinstance(x, str) else x,
            )
        except Exception as exc:
            return Exc(exc, where="compile_task_properties")

        self.runner = runner

        try:
            result = self.runner.config(**runner_config)
            if result is not None and result.is_err:
                self.status = TaskStatus.FAILED
                return result
        except Exception as exc:
            self.status = TaskStatus.FAILED
            return Exc(exc, where="task_config")

        # The config stage can produce changes to: tags, parents, sources, outputs and on_fail
        if hasattr(runner, "_config_input"):
            for source in runner._config_input["sources"]:
                self.src(source)

            for output in runner._config_input["outputs"]:
                self.out(output)

            for parent in runner._config_input["parents"]:
                self.parent_names.add(parent)

            for tag in runner._config_input["tags"]:
                self.tags.add(tag)

            if runner._config_input.get("on_fail") is not None:
                self.on_fail = runner._config_input["on_fail"]

            # A bit of cleaning on the user
            del runner._config_input

        # TODO relying on the task object having a certain property is not a solid method. Need to change it
        if "_target_db" in runner.__dict__:
            target_connection = self.connections[runner._target_db]
        else:
            target_connection = self.connections[self.default_db]

        self.outputs.update(
            {
                self.db_object_compiler.from_string(o, connection=target_connection)
                for o in self.outputs_yaml
            }
        )

        if "_source_db" in runner.__dict__:
            source_connection = self.connections[runner._source_db]
        elif "_target_db" in runner.__dict__:
            source_connection = self.connections[runner._target_db]
        else:
            source_connection = self.connections[self.default_db]

        self.sources.update(
            {
                self.db_object_compiler.from_string(o, connection=source_connection)
                for o in self.sources_yaml
            }
        )

        self.status = TaskStatus.READY_FOR_SETUP

        self.verify_connections()

        return Ok()

    def set_parameters(
        self, project_parameters: Dict[str, Any], task_parameters: Dict[str, Any]
    ):
        # Process parameters
        # The project parameters go as they come
        self.project_parameters = deepcopy(project_parameters or dict())

        # Compile nested dictionary of parameters
        task_parameters = task_parameters or dict()
        task_parameters = map_nested(
            task_parameters,
            lambda x: self.compiler.compile(x) if isinstance(x, str) else x,
        )
        self.task_parameters = task_parameters

        # Add the task paramters to the jinja environment
        self.compiler.update_globals(**task_parameters)

    def check_skip(self):
        if self.run_arguments["command"] != "test":
            failed_parents = {
                p.name: p.status
                for p in self.parents
                if (
                    p.status in (TaskStatus.SETUP_FAILED, TaskStatus.FAILED)
                    and p.on_fail != "no_skip"
                )
                or p.status == TaskStatus.SKIPPED
            }
        else:
            failed_parents = {}

        if len(failed_parents) > 0:
            self.status = TaskStatus.SKIPPED

            if self.in_query:
                return Err("task", "parent_errors", failed_parents=failed_parents)
            else:
                return Ok(self.status)

        elif self.status == TaskStatus.SETUP_FAILED:
            return Err("task", "setup_error", status=self.status)
        elif "fail" in self.status.value:
            return Err("task", "task_error", status=self.status)
        else:
            return Ok(self.status)

    def setup(self, in_query, sources_from_prod):
        # Check the parents are in a good state
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        if self.runner is None:
            return Ok()

        self.in_query = in_query
        self.status = TaskStatus.SETTING_UP

        if not in_query:
            self.status = TaskStatus.NOT_IN_QUERY
            return Ok()
        else:
            # Clear all the dummy connections
            self.runner.connections = {
                n: None if isinstance(o, UnknownDb) else o
                for n, o in self.runner.connections.items()
            }

            needs_recompile = False
            for s in self.sources:
                if s in sources_from_prod:
                    needs_recompile = True

            self.runner._needs_recompile = needs_recompile

            self.sources_from_prod = sources_from_prod

            # Run the setup stage for the runner and return the results
            try:
                result = self.runner.setup()
            except Exception as exc:
                result = Exc(exc)

            finally:
                if result is None:
                    self.status = TaskStatus.READY
                    return Ok()
                elif not isinstance(result, Result):
                    self.status = TaskStatus.SETUP_FAILED
                    self.tracker.current_step = None
                    return Err("task_result", "missing_result_error", result=result)
                elif result.is_err:
                    self.status = TaskStatus.SETUP_FAILED
                    self.tracker.current_step = None
                    return result
                else:
                    self.status = TaskStatus.READY
                    return Ok()

    def should_run(self):
        return self.status == TaskStatus.NOT_IN_QUERY

    def run(self):
        return self.execute_task("run")

    def compile(self):
        return self.execute_task("compile")

    def test(self):
        return self.execute_task("test")

    def execute_task(self, command):
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        if self.runner is None:
            return Ok()

        if not self.in_query:
            # TODO review this as it should never happend if running sayn cli
            self.status = TaskStatus.NOT_IN_QUERY
            return Err("execution", "task_not_in_query")
        elif self.status not in (TaskStatus.SETTING_UP, TaskStatus.READY):
            return Err("execution", "setup_error", status=self.status)
        else:
            try:
                if command == "run":
                    result = self.runner.run()
                elif command == "compile":
                    result = self.runner.compile()
                else:
                    result = self.runner.test()

                if result is None:
                    result = Ok()
                    self.status = TaskStatus.SUCCEEDED
                elif result.is_ok:
                    self.status = TaskStatus.SUCCEEDED
                else:
                    self.status = TaskStatus.FAILED
            except Exception as e:
                self.status = TaskStatus.FAILED
                result = Exc(e)

            return result

    def set_parents(self, all_tasks, output_to_task):
        for parent_name in self.parent_names:
            if parent_name not in all_tasks:
                return Err("dag", "missing_parents", missing={self.name: [parent_name]})
            self.parents.append(all_tasks[parent_name])
            self.parent_names.add(parent_name)

        missing = set()
        for source in self.sources:
            if source not in output_to_task:
                if source.connection_name == self.default_db:
                    # We only consider a missing parent if the
                    # source is in the default_db
                    missing.add(source)
            else:
                for task_name in output_to_task[source]:
                    if task_name not in self.parent_names:
                        self.parents.append(all_tasks[task_name])
                        self.parent_names.add(task_name)

        # TODO send some message when a table is source
        if len(missing) > 0:
            tables = ", ".join([f"{t.raw}" for t in missing])
            self.tracker.warning(
                f'No task creates table(s) "{tables}" referenced by task "{self.name}"'
            )

        return Ok()

    def src(self, obj, connection=None):
        obj = self.db_object_compiler.from_string(obj, connection=connection)
        if self.status == TaskStatus.CONFIGURING:
            # During configuration we add to the list and use values based on settings
            self.used_connections.add(obj.connection_name)
            self.sources.add(obj)

        return self.db_object_compiler.src_value(obj)

    def out(self, obj, connection=None):
        obj = self.db_object_compiler.from_string(obj, connection=connection)
        if self.status == TaskStatus.CONFIGURING:
            self.used_connections.add(obj.connection_name)
            self.outputs.add(obj)

        return self.db_object_compiler.out_value(obj)

    def verify_connections(self):
        if hasattr(self.runner, "_target_db"):
            self.used_connections.add(self.runner._target_db)

        if hasattr(self.runner, "_source_db"):
            self.used_connections.add(self.runner._source_db)
        return Ok()

    def has_tests(self):
        if self.runner is not None and self.runner._has_tests:
            return True

        return False
