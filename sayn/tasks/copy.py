import logging

from .task import TaskStatus
from .sql import SqlTask


class CopyTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_ddl(type_required=False)
        if self.ddl is None:
            # TODO full copy of table when ddl not specified
            return self.failed("DDL is required for copy tasks")

        status = self._setup_source()
        if status != TaskStatus.READY:
            return status

        status = self._setup_destination()
        if status != TaskStatus.READY:
            return status

        status = self._setup_incremental()
        if status != TaskStatus.READY:
            return status

        status = self._setup_table_columns()
        if status != TaskStatus.READY:
            return status

        status = self._setup_sql()
        if status != TaskStatus.READY:
            return status

        return self.ready()

    def run(self):
        logging.debug("Writting query on disk")
        status = self._write_queries()
        if status != TaskStatus.SUCCESS:
            return self.failed()

        logging.debug("Running SQL")
        try:
            step = "last_incremental_value"
            last_incremental_value = None
            if self.compiled[step] is not None:
                res = self.db.select(self.compiled[step])
                res = list(res[0].values())[0]
                if res is not None:
                    last_incremental_value = {'incremental_value': res}

            step = "get_data"
            data = self.source_db.select(self.compiled[step], last_incremental_value)

            if len(data) == 0:
                logging.debug('Nothing to load')
                return self.success()

            step = "create_load_table"
            if self.compiled[step] is not None:
                self.db.execute(self.compiled[step])

            step = "load_data"
            self.db.load_data(self.load_table, self.load_schema, data)

            step = "finish"
            if self.compiled[step] is not None:
                self.db.execute(self.compiled[step])
        except Exception as e:
            logging.error(f'Error running step "{step}"')
            logging.error(e)
            if step != "load_data":
                logging.debug(f"Query: {self.compiled[step]}")
            return self.failed()

        return self.success()

    def compile(self):
        status = self._write_queries()
        if status != TaskStatus.SUCCESS:
            return self.failed()
        else:
            return self.success()

    def _write_queries(self):
        try:

            step = "last_incremental_value"
            if self.compiled[step] is not None:
                self._write_query(self.compiled[step], suffix=step)

            step = "get_data"
            self._write_query(self.compiled[step], suffix=step)

            step = "create_load_table"
            if self.compiled[step] is not None:
                self._write_query(self.compiled[step], suffix=step)

            step = "finish"
            if self.compiled[step] is not None:
                self._write_query(self.compiled[step], suffix=step)
        except Exception as e:
            return self.failed((f'Error saving query "{step}" on disk', e))

        return TaskStatus.SUCCESS

    # Task property methods

    def _setup_incremental(self):
        # Type of materialisation
        self.delete_key = self._pop_property("delete_key")
        self.incremental_key = self._pop_property("incremental_key")
        
        if (self.delete_key is None) != (self.incremental_key is None):
            return self.failed(
                f'Incremental copy requires both "delete_key" and "incremental_key"'
            )

        return TaskStatus.READY

    def _setup_source(self):
        # Source property indicating the table this will create
        source = self._pop_property(
            "source", default={"schema": None}
        )

        self.schema = source.pop('schema', None)
        if self.schema is not None and isinstance(self.schema, str):
            self.schema = self.compile_property(self.schema)
        else:
            return self.failed('Optional property "schema" must be a string')

        if set(source.keys()) != set(["db", "table"]) or source['db'] is None or source['table'] is None:
            return self.failed(
                'Source requires "table" and "db" fields. Optional field: "schema".'
            )
        else:
            source_db_name = self.compile_property(source.pop("db"))
            self.source_table = self.compile_property(source.pop("table"))

        if source_db_name not in self.sayn_config.dbs:
            return self.failed(f'{source_db_name} is not a valid vallue for "db" in "source"')
        else:
            self.source_db = self.sayn_config.dbs[source_db_name]

        return TaskStatus.READY

    def _setup_table_columns(self):
        self.source_table_def = self.source_db.get_table(
            self.source_table,
            self.source_schema,
            columns=[c["name"] for c in self.ddl["columns"]],
            required_existing=True
        )

        if self.source_table_def is None or not self.source_table_def.exists():
            return self.failed(
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
                ].type.compile()

        return TaskStatus.READY

    # Utility methods

    def _setup_sql(self):
        self.destination_table_def = self.db.get_table(
            self.table, self.schema, columns=self.source_table_def.columns
        )

        if self.destination_table_def is None:
            return self.failed('Error detecting destination table')

        if not self.destination_table_def.exists():
            # Full load:
            # Create final table directly and load there
            self.load_table = self.table
            self.load_schema = self.schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl
            )
            self.compiled = {
                "last_incremental_value": None,
                "get_data": self.source_db.get_data(
                    self.source_table,
                    self.source_schema,
                    [c["name"] for c in self.ddl["columns"]],
                ),
                "create_load_table": f"-- Create table\n{create_load_table}",
                "finish": None,
            }

        elif self.incremental_key is None:
            # Table exists and it's not an incremental load:
            # Create temporary table, load there and then move
            self.load_table = self.tmp_table
            self.load_schema = self.tmp_schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl, replace=True
            )
            self.compiled = {
                "last_incremental_value": None,
                "get_data": self.source_db.get_data(
                    self.source_table,
                    self.source_schema,
                    [c["name"] for c in self.ddl["columns"]],
                ),
                "create_load_table": f"-- Create temporary table\n{create_load_table}",
                "finish": self.db.move_table(
                    self.load_table,
                    self.load_schema,
                    self.table,
                    self.schema,
                    self.ddl,
                ),
            }
        elif self.delete_key is not None:
            # Incremental load:
            # Create temporary table, load there and then merge
            self.load_table = self.tmp_table
            self.load_schema = self.tmp_schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl, replace=True
            )
            self.compiled = {
                "last_incremental_value": self.db.get_max_value(
                    self.table, self.schema, self.incremental_key
                ),
                "get_data": self.source_db.get_data(
                    self.source_table,
                    self.source_schema,
                    [c["name"] for c in self.ddl["columns"]],
                    incremental_key=self.incremental_key,
                ),
                "create_load_table": f"-- Create temporary table\n{create_load_table}",
                "finish": self.db.merge_tables(
                    self.load_table,
                    self.load_schema,
                    self.table,
                    self.schema,
                    self.delete_key,
                ),
            }
        else:
            raise ValueError("Sayn error in copy")

        return TaskStatus.READY
