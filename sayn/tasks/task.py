from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Set, Dict, Any

from terminaltables import AsciiTable

from ..core.errors import Err, Ok
from ..database import Database
from ..logging.task_event_tracker import TaskEventTracker
from ..utils.compiler import Compiler


class TaskStatus(Enum):
    NOT_IN_QUERY = "not_in_query"

    CONFIGURING = "config"
    READY_FOR_SETUP = "ready_for_setup"

    SETTING_UP = "setting_up"
    READY = "ready"
    SETUP_FAILED = "setup_failed"

    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"

    UNKNOWN = "unknown"


class Task:
    """
    Base class for tasks in SAYN.

    Attributes:
        name (str): Name of the task as defined in the task group.
        group (str): Name of the task group where the task was defined.
        run_arguments (dict): Dictionary containing the values for the arguments specified in the cli.
        task_parameters (dict): Provides access to the parameters specified in the task.
        project_parameters (dict): Provides access to the global parameters of the project.
        parameters (dict): Convinience property joining project and task parameters.
        connections (dict): Dictionary of connections specified for the project.
        tracker (sayn.logging.TaskEventTracker): Message tracker for the current task.
        jinja_env (jinja2.Environment): Jinja environment for this task. The environment comes pre-populated with the parameter values relevant to the task.
    """

    name: str
    group: str
    tags: Set[str]
    run_arguments: Dict[str, Any]
    task_parameters: Dict[str, Any]
    project_parameters: Dict[str, Any]

    _default_db: str
    connections = dict()
    _tracker: TaskEventTracker

    compiler: Compiler

    _has_tests = False

    # Handy properties
    @property
    def parameters(self):
        return {**self.project_parameters, **self.task_parameters}

    @property
    def default_db(self):
        return self.connections[self._default_db]

    # Making it backwards compatible
    @property
    def logger(self):
        return self._tracker

    # Task lifetime methods

    def config(self, **config):
        raise NotImplementedError(
            "SAYN task", self.__class__.__name__, "setup", str(config)
        )

    def setup(self, needs_recompile: bool):
        raise NotImplementedError(
            "SAYN task", self.__class__.__name__, "setup", needs_recompile
        )

    def run(self):
        raise NotImplementedError("SAYN task", self.__class__.__name__, "run")

    def test(self):
        raise NotImplementedError("SAYN task", self.__class__.__name__, "test")

    def compile(self):
        raise NotImplementedError("SAYN task", self.__class__.__name__, "compile")

    # Status methods

    def __init__(
        self,
        name,
        group,
        tracker,
        run_arguments,
        task_parameters,
        project_parameters,
        default_db,
        connections,
        compiler,
        src,
        out,
    ):
        self.name = name
        self.group = group
        self._tracker = tracker
        self.run_arguments = run_arguments
        self.task_parameters = task_parameters
        self.project_parameters = project_parameters
        self._default_db = default_db
        self.connections = connections
        self.compiler = compiler
        self.src = src
        self.out = out

        # This is a dictionary of configuration that can be passed from the runner to the wrapper
        self._config_input = {
            "sources": set(),
            "outputs": set(),
            "parents": set(),
            "tags": set(),
            "on_fail": None,
            "task_name": None,
        }

    def ready(self):
        """(Deprecated: use `success` instead) Returned on successful execution."""
        return Ok()

    def success(self):
        """Returned on successful execution."""
        return Ok()

    def fail(self, msg=None):
        """Returned on failure in any stage."""
        if msg is None:
            msg = 'Unknown error. Use `self.fail("Error message")` in python tasks for more details.'
        return Err("tasks", "task_fail", message=msg)

    # Logging methods

    def set_run_steps(self, steps):
        """Sets the run steps for the task, allowing the CLI to indicate task execution progress."""
        self._tracker.set_run_steps(steps)

    def add_run_steps(self, steps):
        """Adds new steps to the list of run steps for the task, allowing the CLI to indicate task execution progress."""
        self._tracker.add_run_steps(steps)

    def start_step(self, step):
        """Specifies the start of a task step"""
        self._tracker.start_step(step)

    def finish_current_step(self, result=Ok()):
        """Specifies the end of the current step"""
        self._tracker.finish_current_step(result)

    def debug(self, message, details=None):
        """Print a debug message when executing sayn in debug mode (`sayn run -d`)"""
        if details is None:
            details = dict()
        self._tracker.debug(message, **details)

    def info(self, message, details=None):
        """Prints an info message."""
        if details is None:
            details = dict()
        self._tracker.info(message, **details)

    def warning(self, message, details=None):
        """Prints a warning message which will be persisted on the screen after the task concludes execution."""
        if details is None:
            details = dict()
        self._tracker.warning(message, **details)

    def error(self, message, details=None):
        """Prints an error message which will be persisted on the screen after the task concludes execution.

        Executing this method doesn't abort the task or changes the task status. Use `return self.fail` for that instead.

        Args:
          message (str): An optinal error message to print to the screen.

        """
        if details is None:
            details = dict()
        self._tracker.error(message, **details)

    @contextmanager
    def step(self, step):
        """Step context

        Usage:
        ```python
        with self.step('Generate Data'):
            data = generate_data()
        ```

        Args:
          step (str): name of the step being executed.
        """
        self._tracker.start_step(step)
        yield
        self._tracker.finish_current_step()

    def get_test_breakdown(self, breakdown):
        data = []
        data.append(["Status", "Test Type", "Fields Tested"])
        for brk in breakdown:
            if "print" in brk:
                data.append([" ", " ", " "])
            elif not brk["execute"]:
                data.append(["SKIPPED", brk["type"], brk["column"]])
            elif brk["status"] and brk["execute"]:
                data.append(["RESOLVED", brk["type"], brk["column"]])
            else:
                data.append(["EXECUTED", brk["type"], brk["column"]])

        table = AsciiTable(data)
        return "\n" + table.table

    # Jinja methods

    def compile_obj(self, obj, **params):
        """Compiles the object into a string using the task jinja environment.

        Args:
          obj (str/Path/Template): The object to compile. If the object is not a Jinja template object, `self.get_template` will be called first.
          params (dict): An optional dictionary of additional values to use for compilation.
              Note: Project and task parameters values are already set in the environment, so there's no need to pass them on
        """
        return self.compiler.compile(obj, **params)

    def write_compilation_output(self, content, suffix=None, extension="sql"):
        """Writes text content into the compilation folder

        The file will be stored in a folder with the name of the group the task
        belongs to and the name of the file will be that of the task name.
        """
        path = Path(
            self.run_arguments["folders"]["compile"],
            self.group,
            Path(f"{self.name}{'_'+suffix if suffix is not None else ''}.{extension}"),
        )

        # Ensure the path exists and it's empty
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        path.write_text(str(content))
