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
    name = None
    dag = None
    tags = list()
    run_arguments = dict()
    task_parameters = dict()
    project_parameters = dict()

    _default_db = None
    connections = dict()
    logger = None

    jinja_env = None

    # Handy properties
    @property
    def parameters(self):
        return dict(**self.project_parameters, **self.task_parameters)

    @property
    def default_db(self):
        return self.connections[self._default_db]

    @property
    def db(self):
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
        """Returned on successful execution
        """
        return Ok()

    def success(self):
        """Returned on successful execution
        """
        return Ok()

    def fail(self, msg):
        """Returned on failure in any stage
        """
        return Err("tasks", "task_fail", message=msg)

    # Logging methods

    def set_run_steps(self, steps):
        self.logger.set_run_steps(steps)

    def start_step(self, step):
        self.logger.start_step(step)

    def finish_current_step(self, result=Ok()):
        self.logger.finish_current_step(result)

    def debug(self, message, details=None):
        if details is None:
            details = dict()
        self.logger.debug(message, **details)

    def info(self, message, details=None):
        if details is None:
            details = dict()
        self.logger.info(message, **details)

    def warning(self, message, details=None):
        if details is None:
            details = dict()
        self.logger.warning(message, **details)

    def error(self, message, details=None):
        if details is None:
            details = dict()
        self.logger.error(message, **details)

    @contextmanager
    def step(self, step):
        self.logger.start_step(step)
        try:
            yield
            self.logger.finish_current_step()
        except Exception as e:
            self.logger.finish_current_step(e)
            raise e

    # Jinja methods

    def get_template(self, obj):
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
