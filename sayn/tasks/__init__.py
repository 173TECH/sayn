from contextlib import contextmanager
from enum import Enum
from pathlib import Path

from jinja2 import Template

from ..core.errors import Err, Exc, Ok


class TaskStatus(Enum):
    NOT_IN_QUERY = "not_in_query"

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
        name (str): Name of the task as defined in the dag.
        dag (str): Name of the dag where the task was defined.
        run_arguments (dict): Dictionary containing the values for the arguments specified in the cli.
        task_parameters (dict): Provides access to the parameters specified in the task.
        project_parameters (dict): Provides access to the global parameters of the project.
        parameters (dict): Convinience property joining project and task parameters.
        connections (dict): Dictionary of connections specified for the project.
        tracker (sayn.logging.TaskEventTracker): Message tracker for the current task.
        jinja_env (jinja2.Environment): Jinja environment for this task. The environment comes pre-populated with the parameter values relevant to the task.
    """

    name = None
    dag = None
    tags = list()
    run_arguments = dict()
    task_parameters = dict()
    project_parameters = dict()

    _default_db = None
    connections = dict()
    tracker = None

    jinja_env = None

    # Handy properties
    @property
    def parameters(self):
        return {**self.project_parameters, **self.task_parameters}

    @property
    def db(self):
        return self.connections[self._default_db]

    # Making it backwards compatible
    @property
    def logger(self):
        return self.tracker

    @property
    def default_db(self):
        return self.connections[self._default_db]

    # Task lifetime methods

    # def setup(self):
    #     raise NotImplementedError("SAYN task", self.__class__.__name__, "setup")

    # def run(self):
    #     raise NotImplementedError("SAYN task", self.__class__.__name__, "run")

    # def compile(self):
    #     raise NotImplementedError("SAYN task", self.__class__.__name__, "compile")

    # Status methods

    def ready(self):
        """(Deprecated: use `success` instead) Returned on successful execution.
        """
        return Ok()

    def success(self):
        """Returned on successful execution.
        """
        return Ok()

    def fail(self, msg=None):
        """Returned on failure in any stage.
        """
        if msg is None:
            msg = 'Unknown error. Use `self.fail("Error message")` in python tasks for more details.'
        return Err("tasks", "task_fail", message=msg)

    # Logging methods

    def set_run_steps(self, steps):
        """Sets the run steps for the task, allowing the CLI to indicate task execution progress. """
        self.tracker.set_run_steps(steps)

    def start_step(self, step):
        """Specifies the start of a task step"""
        self.tracker.start_step(step)

    def finish_current_step(self, result=Ok()):
        """Specifies the end of the current step"""
        self.tracker.finish_current_step(result)

    def debug(self, message, details=None):
        """Print a debug message when executing sayn in debug mode (`sayn run -d`)"""
        if details is None:
            details = dict()
        self.tracker.debug(message, **details)

    def info(self, message, details=None):
        """Prints an info message."""
        if details is None:
            details = dict()
        self.tracker.info(message, **details)

    def warning(self, message, details=None):
        """Prints a warning message which will be persisted on the screen after the task concludes execution."""
        if details is None:
            details = dict()
        self.tracker.warning(message, **details)

    def error(self, message, details=None):
        """Prints an error message which will be persisted on the screen after the task concludes execution.

        Executing this method doesn't abort the task or changes the task status. Use `return self.fail` for that instead.

        Args:
          message (str): An optinal error message to print to the screen.

        """
        if details is None:
            details = dict()
        self.tracker.error(message, **details)

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
        self.tracker.start_step(step)
        yield
        self.tracker.finish_current_step()

    # Jinja methods

    def get_template(self, obj):
        """Returns a Jinja template object.

        Args:
          obj (str/Path): The object to transform into a template. If a `pathlib.Path` is specified, the template will be read from disk.

        """
        if isinstance(obj, Path):
            try:
                template = obj.read_text()
            except Exception as e:
                return Err("tasks", "get_template_error", file_path=obj, exception=e)
        elif isinstance(obj, str):
            template = str
        else:
            return Err("tasks", "get_template_error", obj=obj)

        try:
            return Ok(self.jinja_env.from_string(template))
        except Exception as e:
            return Exc(e, template=template)

    def compile_obj(self, obj, **params):
        """Compiles the object into a string using the task jinja environment.

        Args:
          obj (str/Path/Template): The object to compile. If the object is not a Jinja template object, `self.get_template` will be called first.
          params (dict): An optional dictionary of additional values to use for compilation.
              Note: Project and task parameters values are already set in the environment, so there's no need to pass them on
        """
        if isinstance(obj, Template):
            template = obj
        else:
            result = self.get_template(obj)
            if result.is_err:
                return result
            else:
                template = result.value

        try:
            return Ok(template.render(**params))
        except Exception as e:
            return Exc(e, template=template, params=params)

    def write_compilation_output(self, content, suffix=None):
        path = Path(
            self.run_arguments["folders"]["compile"],
            self.dag,
            Path(f"{self.name}{'_'+suffix if suffix is not None else ''}.sql"),
        )

        # Ensure the path exists and it's empty
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        path.write_text(str(content))

        return Ok()


class PythonTask(Task):
    def setup(self):
        self.debug("Nothing to be done")
        return self.success()

    def run(self):
        self.debug("Nothing to be done")
        return self.success()

    def compile(self):
        self.debug("Nothing to be done")
        return self.success()
