from copy import deepcopy
import re
from typing import Dict, Any, Set

from sayn.database import Database

from ..core.errors import Err, Exc, Ok, Result
from ..utils.misc import map_nested

from .task import Task, TaskStatus, TaskJinjaEnv

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


# Regular expressions to parse a db object specification
RE_OBJ = re.compile(
    r"((?P<connection>[^.]+):)?((?P<c1>[^.]+)\.)?((?P<c2>[^.]+)\.)?(?P<c3>[^.]+)"
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
    project_parameters: Dict[str, Any]
    task_parameters: Dict[str, Any]
    in_query: bool = False
    runner: Task
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
    ):
        self.tags = set(tags or set())
        self.parent_names = set(parent_names or set())
        self.parents = list()
        self.sources_yaml = set(sources or set())
        self.sources = set()
        self.outputs_yaml = set(outputs or set())
        self.outputs = set()
        self.sources_from_prod = set()
        self.tracker = tracker

        self.name = name
        self.group = group
        self.task_type = task_type
        self.on_fail = on_fail or "skip"

        self.default_db = default_db
        self.connections = dict(connections)

        self.task_class = task_class

        if self.task_class is None:
            self.status = TaskStatus.FAILED
        else:
            self.status = TaskStatus.CONFIGURING

            self.compiler = compiler.get_task_compiler(
                TaskJinjaEnv(group=self.group, name=self.name)
            )

            self.run_arguments = {
                "debug": run_arguments.debug,
                "full_load": run_arguments.full_load,
                "start_dt": run_arguments.start_dt,
                "end_dt": run_arguments.end_dt,
                "command": run_arguments.command.value,
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

        result = self.runner.config(**runner_config)
        if result is not None and result.is_err:
            self.status = TaskStatus.FAILED
            return result

        # TODO relying on the task object having a certain property is not a solid method. Need to change it
        if "_target_db" in runner.__dict__:
            target_connection = self.connections[runner._target_db]
        else:
            target_connection = self.connections[self.default_db]

        self.outputs.update(
            {
                self.get_db_obj(o, connection=target_connection)
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
                self.get_db_obj(o, connection=source_connection)
                for o in self.sources_yaml
            }
        )

        # To get parents for decorators
        if "temp_parents" in runner.__dict__:
            if isinstance(runner.temp_parents, str):
                self.parent_names.add(runner.temp_parents)
            elif isinstance(runner.temp_parents, list):
                self.parent_names.update(runner.temp_parents)
            del runner.temp_parents

        self.status = TaskStatus.READY_FOR_SETUP

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
        failed_parents = {
            p.name: p.status
            for p in self.parents
            if (p.status == TaskStatus.FAILED and p.on_fail != "no_skip")
            or p.status == TaskStatus.SKIPPED
        }

        if len(failed_parents) > 0:
            self.status = TaskStatus.SKIPPED

            if self.in_query:
                return Err("task", "parent_errors", failed_parents=failed_parents)
            else:
                return Ok(self.status)

        elif "fail" in self.status.value:
            return Err("task", "task_error", status=self.status)
        else:
            return Ok(self.status)

    def setup(self, in_query, sources_from_prod):
        # Check the parents are in a good state
        result = self.check_skip()
        if result.is_err or result.value == TaskStatus.SKIPPED:
            return result

        self.in_query = in_query
        self.status = TaskStatus.SETTING_UP

        if not in_query:
            self.status = TaskStatus.NOT_IN_QUERY
            return Ok()
        else:
            needs_recompile = False
            for s in self.sources:
                if s in sources_from_prod:
                    needs_recompile = True

            self.sources_from_prod = sources_from_prod

            # Run the setup stage for the runner and return the results
            try:
                result = self.runner.setup(needs_recompile)
            except Exception as exc:
                result = Exc(exc)

            finally:
                if result is None:
                    self.status = TaskStatus.READY
                    return Ok()
                elif not isinstance(result, Result):
                    self.status = TaskStatus.FAILED
                    self.tracker.current_step = None
                    return Err("task_result", "missing_result_error", result=result)
                elif result.is_err:
                    self.status = TaskStatus.FAILED
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
                    self.status = TaskStatus.SUCCEEDED
                if result.is_ok:
                    self.status = TaskStatus.SUCCEEDED
                else:
                    self.status = TaskStatus.FAILED
            except Exception as e:
                self.status = TaskStatus.FAILED
                result = Exc(e)

            return result

    def set_parents(self, all_tasks, output_to_task):
        for parent_name in self.parent_names:
            self.parents.append(all_tasks[parent_name])
            self.parent_names.add(parent_name)

        missing = set()
        for source in self.sources:
            if source not in output_to_task:
                missing.add(source)
            else:
                for task_name in output_to_task[source]:
                    if task_name not in self.parent_names:
                        self.parents.append(all_tasks[task_name])
                        self.parent_names.add(task_name)

        # TODO send some message when a table is source
        # if len(missing) > 0:
        #     tables = ", ".join([f"{t.get_value()}" for t in missing])
        #     self.status = TaskStatus.SETUP_FAILED
        #     return Err(
        #         "dag",
        #         "missing_sources",
        #         error_message=f'No task creates table(s) "{tables}" referenced by task "{self.name}"',
        #     )

        return Ok()

    def get_db_obj(self, obj: str, connection=None):
        obj = self.compiler.compile(obj)
        m = RE_OBJ.match(obj)
        if m is None:
            raise ValueError(f'Incorrect format for database object "{obj}"')
        else:
            g = m.groupdict()
            connection_name = g["connection"]
            components = dict(
                {"object": None, "schema": None, "database": None},
                **dict(
                    zip(
                        ("object", "schema", "database"),
                        reversed(
                            [
                                v
                                for k, v in g.items()
                                if k != "connection" and v is not None
                            ]
                        ),
                    )
                ),
            )

            if connection is not None:
                if isinstance(connection, str):
                    connection_object = self.connections[connection]
                elif isinstance(connection, Database):
                    connection_object = connection
                else:
                    raise ValueError(f"Wrong type {type(connection)}")
            else:
                connection_object = self.connections[connection_name or self.default_db]

            return connection_object._object_builder.from_components(**components)

    def src(self, obj, connection=None):
        if self.status != TaskStatus.CONFIGURING:
            print(self.name)
            # raise ValueError()
        obj = self.get_db_obj(obj, connection=connection)

        if self.status == TaskStatus.CONFIGURING:
            # During configuration we add to the list and use values based on settings
            self.sources.add(obj)
            return obj.get_value()
        else:
            # In any other stage, we check to see is the object is in the list
            # of objects to use from production
            if obj in self.sources_from_prod:
                return obj.get_prod_value()
            else:
                return obj.get_value()

    def out(self, obj, connection=None):
        obj = self.get_db_obj(obj, connection=connection)
        if self.status == TaskStatus.CONFIGURING:
            self.outputs.add(obj)
        return obj.get_value()
