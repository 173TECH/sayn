import logging

from .task import TaskStatus
from .sql import SqlTask
from ..utils import yaml


class CopyTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db
        self._setup_ddl()

        if self.ddl is None:
            # TODO full copy of table when ddl not specified
            return self.failed("DDL is required for copy tasks")

        schema = yaml.Map(
            {
                "from": yaml.Map(
                    {
                        "db": yaml.Enum(self.sayn_config.dbs.keys()),
                        yaml.Optional("schema"): yaml.NotEmptyStr(),
                        "table": yaml.NotEmptyStr(),
                    }
                ),
                "to": yaml.Map(
                    {
                        yaml.Optional("staging_schema"): yaml.NotEmptyStr(),
                        yaml.Optional("schema"): yaml.NotEmptyStr(),
                        "table": yaml.NotEmptyStr(),
                    }
                ),
                yaml.Optional("incremental_key"): yaml.Enum(
                    [c["name"] for c in self.ddl["columns"]]
                ),
                yaml.Optional("delete_key"): yaml.Enum(
                    [c["name"] for c in self.ddl["columns"]]
                ),
            }
        )

        try:
            yaml.as_document(self._task_def, schema)
        except yaml.ValidationError as e:
            return self.failed(["Error in task definition", e])

        self.from_db = self.sayn_config.dbs[self._task_def["from"]["db"]]
        self.from_schema = self.compile_property(self._task_def["from"].get("schema"))
        self.from_table = self.compile_property(self._task_def["from"]["table"])

        self.schema = self.compile_property(self._task_def["to"].get("schema"))
        self.table = self.compile_property(self._task_def["to"]["table"])

        self.staging_schema = self.compile_property(
            self._task_def["to"].get("staging_schema")
        )
        self.staging_table = "sayn_tmp_" + self.table

        self.incremental_key = self._task_def.get("incremental_key")
        self.delete_key = self._task_def.get("delete_key")

        if not (
            (self.incremental_key is None and self.delete_key is None)
            or (not self.incremental_key is None and not self.delete_key is None)
        ):
            return self.failed(
                f'Incremental copy requires both "delete_key" and "incremental_key"'
            )

        self.from_table_def = self.from_db.get_table(
            self.from_table,
            self.from_schema,
            columns=[c["name"] for c in self.ddl["columns"]],
        )

        if self.from_table_def is None or not self.from_table_def.exists():
            return self.failed(
                (
                    f"Table \"{self.from_schema+'.' if self.from_schema is not None else ''}{self.from_table}\""
                    f" does not exists or columns don't match with DDL specification"
                )
            )

        # Fill up column types from the source table
        for column in self.ddl["columns"]:
            if "type" not in column:
                column["type"] = self.from_table_def.columns[
                    column["name"]
                ].type.compile()

        status = self._setup_sql()
        if status != TaskStatus.READY:
            return status

        return self.ready()

    def _setup_ddl(self):
        return super()._setup_ddl(type_required=False)

    def run(self):
        logging.debug("Writting query on disk")
        status = self._write_queries()
        if status != TaskStatus.FINISHED:
            return self.failed()

        logging.debug("Running SQL")
        try:
            step = "last_incremental_value"
            if self.compiled[step] is not None:
                res = self.db.select(self.compiled[step])
                res = list(res[0].values())[0]
                if res is None:
                    last_incremental_value = None
                else:
                    last_incremental_value = {'incremental_value': res}

            step = "get_data"
            data = self.from_db.select(self.compiled[step], last_incremental_value)

            if len(data) == 0:
                logging.debug('Nothing to load')
                return self.finished()

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

        return self.finished()

    def compile(self):
        status = self._write_queries()
        if status != TaskStatus.FINISHED:
            return self.failed()
        else:
            return self.finished()

    def _write_queries(self):
        try:

            step = "last_incremental_value"
            if self.compiled[step] is not None:
                self.write_query(self.compiled[step], suffix=step)

            step = "get_data"
            self.write_query(self.compiled[step], suffix=step)

            step = "create_load_table"
            if self.compiled[step] is not None:
                self.write_query(self.compiled[step], suffix=step)

            step = "finish"
            if self.compiled[step] is not None:
                self.write_query(self.compiled[step], suffix=step)
        except Exception as e:
            return self.failed((f'Error saving query "{step}" on disk', e))

        return TaskStatus.FINISHED

    # Utility methods

    def _setup_sql(self):
        self.to_table_def = self.db.get_table(
            self.table, self.schema, columns=self.from_table_def.columns
        )

        if self.to_table_def is None:
            return self.failed()

        if not self.to_table_def.exists():
            # Full load:
            # Create final table directly and load there
            self.load_table = self.table
            self.load_schema = self.schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl
            )
            self.compiled = {
                "last_incremental_value": None,
                "get_data": self.from_db.get_data(
                    self.from_table,
                    self.from_schema,
                    [c["name"] for c in self.ddl["columns"]],
                ),
                "create_load_table": f"-- Create table\n{create_load_table}",
                "finish": None,
            }

        elif self.incremental_key is None:
            # Table exists and it's not an incremental load:
            # Create staging table, load there and then move
            self.load_table = self.staging_table
            self.load_schema = self.staging_schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl, replace=True
            )
            self.compiled = {
                "last_incremental_value": None,
                "get_data": self.from_db.get_data(
                    self.from_table,
                    self.from_schema,
                    [c["name"] for c in self.ddl["columns"]],
                ),
                "create_load_table": f"-- Create staging table\n{create_load_table}",
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
            # Create staging table, load there and then merge
            self.load_table = self.staging_table
            self.load_schema = self.staging_schema
            create_load_table = self.db.create_table_ddl(
                self.load_table, self.load_schema, self.ddl, replace=True
            )
            self.compiled = {
                "last_incremental_value": self.db.get_max_value(
                    self.table, self.schema, self.incremental_key
                ),
                "get_data": self.from_db.get_data(
                    self.from_table,
                    self.from_schema,
                    [c["name"] for c in self.ddl["columns"]],
                    incremental_key=self.incremental_key,
                ),
                "create_load_table": f"-- Create staging table\n{create_load_table}",
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
