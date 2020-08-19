from pathlib import Path

from .task import Task, TaskStatus
from ..utils import yaml
from ..utils.ui import UI


class SqlTask(Task):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_file_name()
        if status != TaskStatus.READY:
            return status

        self.template = self._get_query_template()
        if self.template is None:
            return self.failed()

        status = self._setup_query()
        if status != TaskStatus.READY:
            return status

        return self._check_extra_fields()

    def run(self):
        UI().debug("Writting query on disk...")

        self._write_query(self.sql_query)
        if self.status == TaskStatus.FAILED:
            return self.failed()

        UI().debug("Executing SQL task...")

        try:
            self.db.execute(self.sql_query)
        except Exception as e:
            return self.failed(
                ("Error running query", f"{e}", f"Query: {self.sql_query}")
            )

        return self.success()

    def compile(self):
        try:
            self._write_query(self.sql_query)
        except Exception as e:
            return self.failed(("Error saving query on disk", f"{e}"))

        return self.success()

    # Task property methods - SETUP

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
            UI().error(f"{path}: file not found")
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
                f'"tmp_schema" not supported for database of type "{self.db.db_type}"'
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

    def _setup_query(self):
        # Retrieve the select statement compiled with jinja
        try:
            self.sql_query = self.template.render(**self.parameters)
        except Exception as e:
            return self.failed(f"Error compiling template\n{e}")

        return TaskStatus.READY

    # Task property methods - EXECUTION

    def _exeplan_drop(self, table, schema, view=False):
        UI().debug(
            "Dropping {name}...".format(
                name=f"{'' if schema is None else schema +'.'}{table}"
            )
        )
        stop = False
        try:
            drop1 = self.db.drop_table(table, schema, view=view)
            self.db.execute(drop1)
            stop = True
        except:
            pass

        if stop is False:
            try:
                drop2 = self.db.drop_table(table, schema, view=not view)
                self.db.execute(drop2)
            except:
                pass

    def _exeplan_create(self, table, schema, select=None, view=False, ddl=dict()):
        UI().debug(
            "Creating {table_or_view} {name}...".format(
                table_or_view="view" if view is True else "table",
                name=f"{'' if schema is None else schema +'.'}{table}",
            )
        )
        if ddl.get("columns") is None:
            create = self.db.create_table_select(table, schema, select, view=view)
            self.db.execute(create)
        else:
            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            # create table with DDL and insert the output of the select
            create = self.db.create_table_ddl(table, schema, ddl)
            self.db.execute(create)
            if select is not None:
                # we need to reshape the query to ensure that the columns are always in the right order
                insert = self.db.insert(table, schema, select, columns=ddl_column_names)
                self.db.execute(insert)

    def _exeplan_create_indexes(self, table, schema, ddl=dict()):
        if ddl.get("indexes") is not None:
            UI().debug(
                "Creating indexes on table {name}...".format(
                    name=f"{'' if self.schema is None else self.schema +'.'}{self.table}"
                )
            )
            indexes = self.db.create_indexes(table, schema, ddl)
            self.db.execute(indexes)

    def _exeplan_move(self, src_table, src_schema, dst_table, dst_schema, ddl=dict()):
        UI().debug(
            "Moving table {src_name} to {dst_name}...".format(
                src_name=f"{'' if src_schema is None else src_schema +'.'}{src_table}",
                dst_name=f"{'' if dst_schema is None else dst_schema +'.'}{dst_table}",
            )
        )
        move = self.db.move_table(src_table, src_schema, dst_table, dst_schema, ddl)
        self.db.execute(move)

    def _exeplan_merge(self, tmp_table, tmp_schema, table, schema, delete_key):
        UI().debug(
            "Merging into table {name}...".format(
                name=f"{'' if schema is None else schema +'.'}{table}"
            )
        )
        merge = self.db.merge_tables(tmp_table, tmp_schema, table, schema, delete_key)
        self.db.execute(merge)

    def _exeplan_set_permissions(self, table, schema, ddl=dict()):
        if ddl.get("permissions") is not None:
            UI().debug(
                "Setting permissions on {name}...".format(
                    name=f"{'' if schema is None else schema +'.'}{table}"
                )
            )
            permissions = self.db.grant_permissions(table, schema, ddl["permissions"])
            self.db.execute(permissions)

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
