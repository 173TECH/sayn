import logging
from pathlib import Path

from .task import Task, TaskStatus
from ..utils import yaml


class SqlTask(Task):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_file_name()
        if status != TaskStatus.READY:
            return status

        self.template = self._get_query_template()
        if self.template is None:
            return self.failed()

        try:
            self.compiled = self.template.render(**self.parameters)
        except Exception as e:
            return self.failed(f"Error compiling template\n{e}")

        return self._check_extra_fields()

    def run(self):
        logging.debug("Writting query on disk")
        self._write_query(self.compiled)
        if self.status == TaskStatus.FAILED:
            return self.failed()

        logging.debug("Running SQL")
        logging.debug(self.compiled)
        try:
            self.db.execute(self.compiled)
        except Exception as e:
            return self.failed(("Error running query", e, f"Query: {self.compiled}"))

        return self.success()

    def compile(self):
        try:
            self._write_query(self.compiled)
        except Exception as e:
            return self.failed(("Error saving query on disk", e))

        return self.success()

    # Task property methods

    def _setup_file_name(self):
        # file_name pointint to the code for the sql task
        self.file_name = self._pop_property("file_name")
        if self.file_name is None:
            return self.failed('"file_name" is a required field')
        else:
            self.file_name = self.compile_property(self.file_name)

        return TaskStatus.READY

    def _get_query_template(self):
        path = Path(self.sayn_config.sql_path, self.compile_property(self.file_name))

        if not path.is_file():
            logging.error(f"{path}: file not found")
            return

        return self.sayn_config.jinja_env.get_template(str(path))

    def _setup_destination(self):
        # Destination property indicating the table this will create
        destination = self._pop_property(
            "destination", default={"tmp_schema": None, "schema": None}
        )

        self.schema = destination.pop("schema", None)
        if self.schema is not None and isinstance(self.schema, str):
            self.schema = self.compile_property(self.schema)
        elif self.schema is not None:
            return self.failed('Optional property "schema" must be a string')

        self.tmp_schema = destination.pop("tmp_schema", None)
        if "NO SET SCHEMA" in self.db.sql_features and self.tmp_schema is not None:
            return self.failed(
                f'"tmp_schema" not supported for database of type "{self.db.type}"'
            )
        elif self.tmp_schema is not None and isinstance(self.tmp_schema, str):
            self.tmp_schema = self.compile_property(self.tmp_schema)
        elif self.tmp_schema is not None:
            return self.failed('Optional property "tmp_schema" must be a string')
        else:
            self.tmp_schema = self.schema

        if (
            set(destination.keys()) == set(["table"])
            and destination["table"] is not None
        ):
            self.table = self.compile_property(destination.pop("table"))
            self.tmp_table = f"sayn_tmp_{self.table}"
        else:
            return self.failed(
                'Destination requires "table" field. Optional fields: tmp_schema and schema.'
            )

        return TaskStatus.READY

    def _setup_ddl(self, type_required=True):
        ddl = self._pop_property("ddl")
        if ddl is not None:
            if isinstance(ddl, str):
                # TODO external file not implemented
                # parsed = yaml.load(self.compile_property(ddl))
                raise ValueError("External file for ddl not implemented")

            self.ddl = self.db.validate_ddl(ddl, type_required=type_required)

            if self.ddl is None:
                return self.failed("Error processing DDL")
            else:
                return TaskStatus.READY

        else:
            self.ddl = dict()
            return TaskStatus.READY

    # Utility methods

    def _write_query(self, query, suffix=None):
        path = Path(
            self.sayn_config.compile_path,
            self.dag,
            Path(f"{self.name}{'_'+suffix if suffix is not None else ''}.sql"),
        )

        # Ensure the path exists and it's empty
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        path.write_text(str(query))
