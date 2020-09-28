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
        self.config = Config(
            sql_folder=self.run_arguments["folders"]["sql"], file_name=file_name
        )

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        # Set execution steps
        self.steps = ["Write Query"]
        if self.run_arguments["command"] == "run":
            self.steps.append("Execute Query")

        return Ok()

    def run(self):
        return self.execute_steps()

    def compile(self):
        return self.execute_steps()

    def execute_steps(self):
        # For incremental loads, manipulate the "Merge" steps depending on whether
        # the target table exists or not. This is done so we can delay the introspection
        if self.steps[-1] == "Merge" and not self.default_db.table_exists(
            self.table, self.schema
        ):
            self.steps.pop(-1)
            if len(self.ddl["indexes"]) > 0:
                self.steps.append("Create Indexes")
            self.steps.extend(["Cleanup Target", "Move"])

        self.set_run_steps(self.steps)

        for step in self.steps:
            self.start_step(step)
            result = self.execute_step(step)
            self.finish_current_step(result)
            if result.is_err:
                return result
            elif isinstance(result.value, str) and self.run_arguments["debug"]:
                self.write_compilation_output(
                    result.value, step.replace(" ", "_").lower()
                )

        return Ok()

    def execute_step(self, step):
        if self.run_arguments["command"] == "run":
            execute = True
        else:
            execute = False

        if step == "Execute Query":
            if execute:
                return self.default_db.execute(self.sql_query)
            else:
                return Ok()

        elif step == "Write Query":
            return self.write_compilation_output(self.sql_query)

        elif step == "Cleanup":
            return self.drop(self.tmp_table, self.tmp_schema, execute)

        elif step == "Cleanup Target":
            return self.drop(self.table, self.schema, execute)

        elif step == "Create Temp":
            return self.create_select(
                self.tmp_table, self.tmp_schema, self.sql_query, self.ddl, execute
            )

        elif step == "Create Temp DDL":
            return self.default_db.create_table_ddl(
                self.tmp_table, self.tmp_schema, self.ddl, execute=execute
            )

        elif step == "Create View":
            return self.default_db.create_table_select(
                self.table, self.schema, self.sql_query, view=True, execute=execute
            )

        elif step == "Create Indexes":
            return self.default_db.create_indexes(
                self.tmp_table, self.tmp_schema, self.ddl, execute=execute
            )

        elif step == "Merge":
            return self.default_db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
                execute=execute,
            )

        elif step == "Move":
            return self.default_db.move_table(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.ddl,
                execute=execute,
            )

        elif step == "Grant Permissions":
            return self.default_db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"], execute=execute
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
                execute,
            )

        else:
            return Err("task_execution", "unknown_step", step=step)

    # SQL execution steps methods

    def drop(self, table, schema, execute):
        out_sql = list()

        try:
            result = self.default_db.drop_table(
                table, schema, view=False, execute=execute
            )
            if result.is_err:
                out_sql.append(result.error.details["script"])
            else:
                out_sql.append(result.value)
        except:
            pass

        try:
            result = self.default_db.drop_table(
                table, schema, view=True, execute=execute
            )
            if result.is_err:
                out_sql.append(result.error.details["script"])
            else:
                out_sql.append(result.value)
        except:
            pass

        return Ok("\n".join(out_sql))

    def create_select(self, table, schema, select, ddl, execute):
        if len(ddl.get("columns")) == 0:
            result = self.default_db.create_table_select(
                table, schema, select, view=False, execute=execute
            )
            if result.is_err:
                return result
        else:
            # create table with DDL and insert the output of the select
            result = self.default_db.create_table_ddl(
                table, schema, ddl, execute=execute
            )
            if result.is_err:
                return result

            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            result = self.default_db.insert(
                table, schema, select, columns=ddl_column_names, execute=execute
            )
            if result.is_err:
                return result

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
        execute,
    ):
        if not execute:
            return Ok()

        # Get the incremental value
        if (
            incremental_key is None
            or self.run_arguments["full_load"]
            or not self.default_db.table_exists(table, schema)
        ):
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
