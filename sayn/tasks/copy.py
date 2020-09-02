from sqlalchemy import or_, select

from ..core.errors import DatabaseError
from .sql import SqlTask
from . import TaskStatus


class CopyTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_source()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_destination()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_incremental()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_ddl(type_required=False)
        if self.ddl is None or self.ddl.get("columns") is None:
            # TODO full copy of table when ddl not specified
            return self.fail("DDL is required for copy tasks")

        status = self._setup_table_columns()
        if status != TaskStatus.READY:
            return status

        return self.ready()

    def run(self):
        # Create placeholder for tmp data
        self._exeplan_drop(self.tmp_table, self.tmp_schema)
        self._exeplan_create(self.tmp_table, self.tmp_schema, ddl=self.ddl)
        self._exeplan_create_indexes(self.tmp_table, self.tmp_schema, ddl=self.ddl)

        # Check the last incremental value and load into tmp table
        liv = self._exeplan_get_last_incremental_value()
        self._exeplan_stream_data(liv)

        # Final transfer from tmp to dst
        if liv is None:  # table does not exist or no incremental value
            self._exeplan_move(
                self.tmp_table, self.tmp_schema, self.table, self.schema, ddl=self.ddl
            )
            self._exeplan_set_permissions(self.table, self.schema, ddl=self.ddl)
        else:
            self._exeplan_merge(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            )
            self._exeplan_drop(self.tmp_table, self.tmp_schema)

        return self.success()

    def compile(self):
        return self.success()

    # Task property methods - SETUP

    def _setup_incremental(self):
        # Type of materialisation
        self.delete_key = self._pop_property("delete_key")
        self.incremental_key = self._pop_property("incremental_key")

        if (self.delete_key is None) != (self.incremental_key is None):
            return self.fail(
                'Incremental copy requires both "delete_key" and "incremental_key"'
            )

        return  # TODO return TaskStatus.READY

    def _setup_source(self):
        # Source property indicating the table this will create
        source = self._pop_property("source", default={"schema": None})

        self.source_schema = source.pop("schema", None)
        if self.source_schema is not None and isinstance(self.source_schema, str):
            self.source_schema = self.compile_property(self.source_schema)
        else:
            return self.fail('Optional property "schema" must be a string')

        if (
            set(source.keys()) != set(["db", "table"])
            or source["db"] is None
            or source["table"] is None
        ):
            return self.fail(
                'Source requires "table" and "db" fields. Optional field: "schema".'
            )
        else:
            source_db_name = self.compile_property(source.pop("db"))
            self.source_table = self.compile_property(source.pop("table"))

        if source_db_name not in self.sayn_config.dbs:
            return self.fail(
                f'{source_db_name} is not a valid vallue for "db" in "source"'
            )
        else:
            self.source_db = self.sayn_config.dbs[source_db_name]

        return  # TODO return TaskStatus.READY

    def _setup_table_columns(self):
        try:
            self.source_table_def = self.source_db.get_table(
                self.source_table,
                self.source_schema,
                columns=[c["name"] for c in self.ddl["columns"]],
                required_existing=True,
            )
        except DatabaseError as e:
            return self.fail(f"{e}")

        if self.source_table_def is None or not self.source_table_def.exists():
            return self.fail(
                (
                    f"Table \"{self.source_schema+'.' if self.source_schema is not None else ''}{self.source_table}\""
                    f" does not exists or columns don't match with DDL specification"
                )
            )

        # Fill up column types from the source table
        for column in self.ddl["columns"]:
            if "type" not in column:
                column["type"] = self.source_table_def.columns[
                    column["name"]
                ].type.compile(dialect=self.db.engine.dialect)

        return  # TODO return TaskStatus.READY

    # Task property methods - EXECUTION

    def _exeplan_get_last_incremental_value(self):
        full_table_name = (
            f"{'' if self.schema is None else self.schema +'.'}{self.table}"
        )
        self.logger.debug(
            "Getting last {incremental_key} value from table {name}...".format(
                incremental_key=self.incremental_key, name=full_table_name
            )
        )

        table_exists = self.db.table_exists(self.table, self.schema)

        if table_exists is True and self.incremental_key is not None:
            last_incremental_value_query = f"SELECT MAX({self.incremental_key}) AS value FROM {full_table_name} WHERE {self.incremental_key} IS NOT NULL"
            res = self.db.select(last_incremental_value_query)
            if len(res) == 1:
                last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = None
        else:
            last_incremental_value = None

        return last_incremental_value

    def _exeplan_stream_data(self, last_incremental_value):
        self.logger.debug("Streaming data...")
        # Select stream
        get_data_query = select(
            [self.source_table_def.c[c["name"]] for c in self.ddl["columns"]]
        )
        if last_incremental_value is None:
            data_iter = self.source_db.select_stream(get_data_query)
        else:
            query = get_data_query.where(
                or_(
                    self.source_table_def.c[self.incremental_key].is_(None),
                    self.source_table_def.c[self.incremental_key]
                    > last_incremental_value,
                )
            )
            data_iter = self.source_db.select_stream(query)

        # Load
        self.db.load_data_stream(self.tmp_table, self.tmp_schema, data_iter)
