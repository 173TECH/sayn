from .sql import SqlTask

from sqlalchemy.sql import or_, select


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
            return self.failed("DDL is required for copy tasks")

        status = self._setup_table_columns()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_sql()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        return self.ready()

    def run(self):
        # Steps:
        # 0. check table exists
        table_exists = self.db.table_exists(self.table, self.schema)

        # 1. create load
        self._write_query(self.create_query, suffix="_create_load_table")
        self.db.execute(self.create_query)

        # 2. get last incremental value
        if table_exists and self.incremental_key is not None:
            self._write_query(
                self.last_incremental_value_query, suffix="_last_incremental_value"
            )
            res = self.db.select(self.last_incremental_value_query)
            if len(res) == 1:
                last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = None
        else:
            last_incremental_value = None

        # 3. get data
        # Retrieve in streaming
        if last_incremental_value is None:
            self._write_query(self.get_data_query, suffix="_get_data")
            data_iter = self.source_db.select_stream(self.get_data_query)
        else:
            query = self.get_data_query.where(
                or_(
                    self.source_table_def.c[self.incremental_key].is_(None),
                    self.source_table_def.c[self.incremental_key]
                    > last_incremental_value,
                )
            )
            self._write_query(query, suffix="_get_data")
            data_iter = self.source_db.select_stream(query)

        # Load in streaming
        self.db.load_data_stream(self.tmp_table, self.tmp_schema, data_iter)

        # 4. finish
        if table_exists and self.incremental_key is not None:
            self._write_query(f"{self.merge_query}", suffix="_merge")
            self.db.execute(self.merge_query)
        else:
            self._write_query(f"{self.move_query}", suffix="_move")
            self.db.execute(self.move_query)
            if self.permissions_query is not None:
                self._write_query(f"{self.permissions_query}", suffix="_permissions")
                self.db.execute(self.permissions_query)

        return self.success()

    def compile(self):
        self._write_query(self.create_query, suffix="_create_load_table")
        if self.incremental_key is not None:
            self._write_query(
                self.last_incremental_value_query, suffix="_last_incremental_value"
            )
            self._write_query(
                f"{self.get_data_query_all}{self.get_data_query_filter}",
                suffix="_get_data",
            )
        else:
            self._write_query(self.get_data_query_all, suffix="_get_data")
        self._write_query(f"{self.move_query}", suffix="_move")
        self._write_query(f"{self.merge_query}", suffix="_merge")
        if self.permissions_query is not None:
            self._write_query(f"{self.permissions_query}", suffix="_permissions")

        return self.success()

    # Task property methods

    def _setup_incremental(self):
        # Type of materialisation
        self.delete_key = self._pop_property("delete_key")
        self.incremental_key = self._pop_property("incremental_key")

        if (self.delete_key is None) != (self.incremental_key is None):
            return self.failed(
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
            return self.failed('Optional property "schema" must be a string')

        if (
            set(source.keys()) != set(["db", "table"])
            or source["db"] is None
            or source["table"] is None
        ):
            return self.failed(
                'Source requires "table" and "db" fields. Optional field: "schema".'
            )
        else:
            source_db_name = self.compile_property(source.pop("db"))
            self.source_table = self.compile_property(source.pop("table"))

        if source_db_name not in self.sayn_config.dbs:
            return self.failed(
                f'{source_db_name} is not a valid vallue for "db" in "source"'
            )
        else:
            self.source_db = self.sayn_config.dbs[source_db_name]

        return  # TODO return TaskStatus.READY

    def _setup_table_columns(self):
        self.source_table_def = self.source_db.get_table(
            self.source_table,
            self.source_schema,
            columns=[c["name"] for c in self.ddl["columns"]],
            required_existing=True,
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
                ].type.compile(dialect=self.db.engine.dialect)

        return  # TODO return TaskStatus.READY

    def _setup_sql(self):
        # 1. Create: Create the table where we'll load the data
        self.create_query = None
        # 2. Incremental value: Get the last incremental value
        self.last_incremental_value_query = None
        # 3. Get data: retrieves the data from the source table
        self.get_data_query_all = None
        self.get_data_query_filter = None
        # 4. Move: query to moves the tmp table to the target one
        self.move_query = None
        # 5. Merge: query to merge data into the target object
        self.merge_query = None
        # 6. Permissions: grants permissions if necessary
        self.permissions_query = None

        # 1. Create query
        # We always load data into a temporary table
        create_table_ddl = self.db.create_table_ddl(
            self.tmp_table, self.tmp_schema, self.ddl, replace=True
        )

        # Add indexes if necessary
        if self.ddl.get("indexes") is not None:
            indexes = "\n-- Create indexes\n" + self.db.create_indexes(
                self.tmp_table, self.tmp_schema, self.ddl
            )
        else:
            indexes = ""

        self.create_query = create_table_ddl + "\n" + indexes

        # 2. Incremental value
        full_table_name = (
            f"{'' if self.schema is None else self.schema +'.'}{self.table}"
        )
        if self.incremental_key is not None:
            self.last_incremental_value_query = f"SELECT MAX({self.incremental_key}) AS value FROM {full_table_name} WHERE {self.incremental_key} IS NOT NULL"
        else:
            self.last_incremental_value_query = None

        # 3. Get data
        self.get_data_query = select(
            [self.source_table_def.c[c["name"]] for c in self.ddl["columns"]]
        )

        # 4. Move
        self.move_query = self.db.move_table(
            self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl
        )

        # 5. Merge
        self.merge_query = self.db.merge_tables(
            self.tmp_table, self.tmp_schema, self.table, self.schema, self.delete_key,
        )

        # 6. Permissions are always the same
        if self.ddl.get("permissions") is not None:
            self.permissions_query = self.db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"]
            )

        return  # TODO return TaskStatus.READY
