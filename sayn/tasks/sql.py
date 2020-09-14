from pathlib import Path

from pydantic import BaseModel, FilePath, validator
from sqlalchemy import or_, select

from ..core.errors import Err, Ok
from . import Task


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(Task):
    def setup(self, file_name):
        print("adfadfaf")
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"])

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        return Ok()

    def run(self):
        return self.execute_steps(["Write Query", "Execute Query"])

    def compile(self):
        return self.execute_steps(["Write Query"])

    def execute_steps(self, steps):
        self.set_run_steps(steps)

        for step in steps:
            self.start_step(step)
            result = self.execute_step(step)
            self.finish_current_step(result)
            if result.is_err:
                return result

        return Ok()

    def execute_step(self, step):
        if step == "Execute Query":
            return self.default_db.execute(self.sql_query)

        elif step == "Write Query":
            return self.write_compilation_output(self.sql_query)

        elif step == "Cleanup":
            return self.drop(self.tmp_table, self.tmp_schema)

        elif step == "Drop Target":
            return self.drop(self.table, self.schema)

        elif step == "Create Temp":
            return self.create_select(
                self.tmp_table, self.tmp_schema, self.sql_query, self.ddl
            )

        elif step == "Create Temp DDL":
            return self.default_db.create_table_ddl(
                self.tmp_table, self.tmp_schema, self.ddl, execute=True
            )

        elif step == "Create View":
            return self.default_db.create_table_select(
                self.table, self.schema, self.sql_query, view=True, execute=True
            )

        elif step == "Create Indexes":
            return self.default_db.create_indexes(
                self.table, self.schema, self.ddl, execute=True
            )

        elif step == "Merge":
            return self.default_db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
                execute=True,
            )

        elif step == "Move":
            return self.default_db.move_table(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.ddl,
                execute=True,
            )

        elif step == "Grant Permissions":
            return self.default_db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"], execute=True
            )

        elif step == "Load Data":
            return self.load_data(
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
            return Err("task_execution", "unknown_step", step=step)

    # SQL execution steps methods

    def drop(self, table, schema):
        try:
            self.default_db.drop_table(table, schema, view=False, execute=True)
        except:
            pass

        try:
            self.default_db.drop_table(table, schema, view=True, execute=True)
        except:
            pass

        return Ok()

    def create_select(self, table, schema, select, ddl):
        if len(ddl.get("columns")) == 0:
            self.default_db.create_table_select(
                table, schema, select, view=False, execute=True
            )
        else:
            # create table with DDL and insert the output of the select
            self.default_db.create_table_ddl(table, schema, ddl, execute=True)

            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            self.default_db.insert(
                table, schema, select, columns=ddl_column_names, execute=True
            )

        return Ok()

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
        # Get the incremental value
        if incremental_key is None or not self.default_db.table_exists(table, schema):
            last_incremental_value = None
        else:
            res = self.default_db.select(
                (
                    f"SELECT MAX({incremental_key}) AS value\n"
                    f"FROM {'' if schema is None else schema +'.'}{table}\n"
                    f"WHERE {incremental_key} IS NOT NULL"
                )
            )
            if len(res) == 1:
                last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = None

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
        return self.default_db.load_data_stream(tmp_table, tmp_schema, data_iter)
