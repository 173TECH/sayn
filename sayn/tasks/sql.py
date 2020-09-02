from pathlib import Path

from pydantic import BaseModel, FilePath, validator
from sqlalchemy import or_, select

from ..core.errors import Result
from . import Task


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(Task):
    def setup(self, file_name):
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"])

        try:
            self.sql_query = self.compile_obj(self.config.file_name)
        except Exception as e:
            return self.fail(message=f"Error compiling template\n{e}")

        return self.ready()

    def run(self):
        return self.execute_steps(["write_query_on_disk", "execute_sql"])

    def compile(self):
        return self.execute_steps(["write_query_on_disk"])

    def execute_steps(self, steps):
        self.set_run_steps(steps)

        for step in steps:
            self.start_step(step)
            result = self.execute_step(step)
            self.finish_current_step(result)
            if result.is_err:
                return result

    def execute_step(self, step):
        if step == "execute_sql":
            result = self.default_db.execute(self.sql_query)

        elif step == "write_query_on_disk":
            try:
                result = self.write_compilation_output(self.sql_query)
            except Exception as e:
                result = self.fail(("Error saving query on disk", f"{e}"))

        elif step == "drop_tmp":
            result = self.drop(self.tmp_table, self.tmp_schema)

        elif step == "drop":
            result = self.drop(self.table, self.schema)

        elif step == "create_tmp":
            result = self.create_select(
                self.tmp_table, self.tmp_schema, self.sql_query, self.ddl
            )

        elif step == "create_tmp_ddl":
            result = self.create_ddl(self.tmp_table, self.tmp_schema, self.ddl)

        elif step == "create_view":
            self.create_view(self.table, self.schema, self.sql_query)

        elif step == "create_indexes":
            self.create_select(self.table, self.schema, self.sql_query, self.ddl)

        elif step == "merge":
            result = self.merge(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
                self.ddl,
            )

        elif step == "move":
            result = self.move(
                self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl,
            )

        elif step == "set_permissions":
            result = self.set_permissions(self.table, self.schema, self.ddl)

        elif step == "load_data":
            result = self.load_data(
                self.source_table_def,
                self.source_db,
                self.table,
                self.schema,
                self.tmp_table,
                self.tmp_schema,
                self.incremental_key,
                self.ddl,
            )

        else:
            result = Result.Err("task_execution", "unknown_step", {"step": step})

        return result

    # SQL execution steps methods

    def drop(self, table, schema):
        self.logger.debug(f"Dropping {'' if schema is None else schema +'.'}{table}...")

        try:
            self.default_db.drop_table(table, schema, view=False, execute=True)
        except:
            pass

        try:
            self.default_db.drop_table(table, schema, view=True, execute=True)
        except:
            pass

        return self.success()

    def create_view(self, table, schema, select, ddl):
        self.logger.debug(
            f"Creating view {'' if schema is None else schema +'.'}{table}..."
        )
        self.default_db.create_table_select(
            table, schema, select, view=True, execute=True
        )

    def create_select(self, table, schema, select, ddl):
        self.logger.debug(
            f"Creating table {'' if schema is None else schema +'.'}{table}..."
        )
        if ddl.get("columns") is None:
            self.default_db.create_table_select(
                table, schema, select, view=False, execute=True
            )
        else:
            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            # create table with DDL and insert the output of the select
            self.default_db.create_table_ddl(table, schema, ddl, execute=True)
            if select is not None:
                # we need to reshape the query to ensure that the columns are always in the right order
                self.default_db.insert(
                    table, schema, select, columns=ddl_column_names, execute=True
                )

        return self.success()

    def create_ddl(self, table, schema, ddl):
        self.logger.debug(
            f"Creating table {'' if schema is None else schema +'.'}{table}..."
        )
        self.default_db.create_table_ddl(table, schema, ddl, execute=True)

        return self.success()

    def create_indexes(self, table, schema, ddl):
        if ddl.get("indexes") is not None:
            self.logger.debug(
                f"Creating indexes on table {'' if self.schema is None else self.schema +'.'}{self.table}..."
            )
            self.default_db.create_indexes(table, schema, ddl, execute=True)

        return self.success()

    def move(self, src_table, src_schema, dst_table, dst_schema, ddl):
        self.logger.debug(
            "Moving table {src_name} to {dst_name}...".format(
                src_name=f"{'' if src_schema is None else src_schema +'.'}{src_table}",
                dst_name=f"{'' if dst_schema is None else dst_schema +'.'}{dst_table}",
            )
        )
        self.default_db.move_table(
            src_table, src_schema, dst_table, dst_schema, ddl, execute=True
        )

        return self.success()

    def merge(self, tmp_table, tmp_schema, table, schema, delete_key, ddl):
        self.logger.debug(
            "Merging into table {name}...".format(
                name=f"{'' if schema is None else schema +'.'}{table}"
            )
        )
        self.default_db.merge_tables(
            tmp_table, tmp_schema, table, schema, delete_key, execute=True
        )

        return self.success()

    def set_permissions(self, table, schema, ddl):
        if ddl.get("permissions") is not None:
            self.logger.debug(
                f"Setting permissions on {'' if schema is None else schema +'.'}{table}..."
            )
            self.default_db.grant_permissions(
                table, schema, ddl["permissions"], execute=True
            )

        return self.success()

    def get_last_incremental_value(self, table, schema, incremental_key):
        full_table_name = f"{'' if schema is None else schema +'.'}{table}"
        self.logger.debug(
            "Getting last {incremental_key} value from table {name}...".format(
                incremental_key=incremental_key, name=full_table_name
            )
        )

        table_exists = self.default_db.table_exists(table, schema)

        if table_exists is True and incremental_key is not None:
            last_incremental_value_query = f"SELECT MAX({incremental_key}) AS value FROM {full_table_name} WHERE {incremental_key} IS NOT NULL"
            res = self.default_db.select(last_incremental_value_query)
            if len(res) == 1:
                last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = None
        else:
            last_incremental_value = None

        return last_incremental_value

    def load_data(
        self,
        source_table_def,
        source_db,
        table,
        schema,
        tmp_table,
        tmp_schema,
        incremental_key,
        ddl,
    ):
        last_incremental_value = self.get_last_incremental_value(
            table, schema, incremental_key
        )

        self.logger.debug("Streaming data...")

        # Select stream
        get_data_query = select([source_table_def.c[c["name"]] for c in ddl["columns"]])
        if last_incremental_value is None:
            data_iter = source_db.select_stream(get_data_query)
        else:
            query = get_data_query.where(
                or_(
                    source_table_def.c[incremental_key].is_(None),
                    source_table_def.c[incremental_key] > last_incremental_value,
                )
            )
            data_iter = source_db.select_stream(query)

        # Load
        self.default_db.load_data_stream(tmp_table, tmp_schema, data_iter)

        return self.success()
