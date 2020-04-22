import logging
from pathlib import Path

from .task import Task, TaskStatus
from ..utils import yaml


class SqlTask(Task):
    def setup(self):
        self.template = self.get_query()

        if self.template is None:
            return self.failed()

        try:
            self.compiled = self.template.render(**self.parameters)
        except Exception as e:
            return self.failed(f"Error compiling template\n{e}")

        self.db = self.sayn_config.default_db

        return self.ready()

    def run(self):
        logging.debug("Writting query on disk")
        self.write_query(self.compiled)
        if self.status == TaskStatus.FAILED:
            return self.failed()

        logging.debug("Running SQL")
        logging.debug(self.compiled)
        try:
            self.db.execute(self.compiled)
        except Exception as e:
            return self.failed(("Error running query", e, f"Query: {self.compiled}"))

        return self.finished()

    def compile(self):
        try:
            self.write_query(self.compiled)
        except Exception as e:
            return self.failed(("Error saving query on disk", e))

        return self.finished()

    # Utility methods

    def write_query(self, query, suffix=None):
        path = Path(
            self.sayn_config.compile_path,
            Path(f"{self.name}{'_'+suffix if suffix is not None else ''}.sql"),
        )

        # Ensure the path exists and it's empty
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        path.write_text(str(query))

    def get_query(self):
        logging.debug("Setting up SQL Task")

        if "file_name" not in self._task_def:
            logging.error('"file_name" is a required field')
            return

        path = Path(
            self.sayn_config.sql_path,
            self.compile_property(self._task_def["file_name"]),
        )

        if not path.is_file():
            logging.error(f"{path}: file not found")
            return

        logging.debug("Compiling sql")

        return self.sayn_config.jinja_env.get_template(str(path))

    def _setup_ddl(self, type_required=True):
        ddl = self._task_def.pop('ddl', None)
        if ddl is not None:
            if isinstance(ddl, str):
                # TODO external file not implemented
                parsed = yaml.load(self.compile_property(ddl))
            else:
                parsed = yaml.as_document(ddl)

            self.ddl = self.db.validate_ddl(parsed, type_required=type_required)

            if self.ddl is None:
                return self.failed("Error processing DDL")
            else:
                return TaskStatus.READY

        else:
            self.ddl = None
            return TaskStatus.READY
