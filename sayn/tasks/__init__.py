from contextlib import contextmanager
from enum import Enum
from pathlib import Path

from jinja2 import Template

from ..core.errors import Result


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

    # Task lifetime methods

    def setup(self):
        raise NotImplementedError("Setup method not implemented")

    def run(self):
        raise NotImplementedError("Run method not implemented")

    def compile(self):
        raise NotImplementedError("Compile method not implemented")

    def set_run_steps(self, steps):
        self.logger.set_run_steps(steps)

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
            return self.jinja_env.from_string(obj.read_text())
        elif isinstance(obj, str):
            return self.jinja_env.from_string(obj)

    def compile_obj(self, obj, **params):
        if isinstance(obj, Template):
            return obj.render(**params)
        else:
            return self.get_template(obj).render(**params)

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

    # Status methods

    def ready(self):
        return Result.Ok()

    def success(self):
        return Result.Ok()

    def fail(self, message=None, details=None):
        return Result.Err(
            module="tasks", error_code="task_fail", message=message, details=details
        )
