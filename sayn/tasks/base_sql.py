from sqlalchemy import or_, select

from ..core.errors import Err, Exc, Ok
from . import Task


class BaseSqlTask(Task):
    target_table_exists = None

    def run(self):
        return self.execute_steps()

    def compile(self):
        return self.execute_steps()

    def execute_steps(self):
        # For incremental loads, manipulate the "Merge" steps depending on whether
        # the target table exists or not. This is done so we can delay the introspection
        if "Merge" in self.steps and self.run_arguments["command"] == "run":
            self.target_table_exists = self.default_db.table_exists(
                self.table, self.schema
            )

            if not self.target_table_exists:
                tmp = self.steps[self.steps.index("Merge") + 1 :]
                self.steps = self.steps[: self.steps.index("Merge")]
                if len(self.ddl["indexes"]) > 0:
                    self.steps.append("Create Indexes")
                self.steps.extend(["Cleanup Target", "Move"])
                self.steps.extend(tmp)
            else:
                self.steps.append("Cleanup")

        self.set_run_steps(self.steps)

        for step in self.steps:
            with self.step(step):
                result = self.execute_step(step)
                if result.is_err:
                    return result

        return Ok()

    def create_table_ddl(self, tmp_table, tmp_schema, ddl, execute):
        q = self.default_db.create_table_ddl(tmp_table, tmp_schema, ddl, execute)
        if execute:
            self.default_db.execute(q)

        return Ok(q)

    def execute_step(self, step):
        execute = self.run_arguments["command"] == "run"

        get_query_steps = {
            "Create Temp": lambda: self.create_select(
                self.tmp_table, self.tmp_schema, self.sql_query, self.ddl
            ),
            "Create Temp DDL": lambda: self.default_db.create_table_ddl(
                self.tmp_table, self.tmp_schema, self.ddl
            ),
            "Create View": lambda: self.default_db.create_table_select(
                self.table, self.schema, self.sql_query, view=True
            ),
            "Create Indexes": lambda: self.default_db.create_indexes(
                self.tmp_table, self.tmp_schema, self.ddl
            ),
            "Merge": lambda: self.default_db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            ),
            "Move": lambda: self.default_db.move_table(
                self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl,
            ),
            "Grant Permissions": lambda: self.default_db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"]
            ),
        }

        if step in get_query_steps:
            # These steps are always: 1. Get the query, 2. Save to disk, 3. Execute
            # For more complex steps, there are specific entries in this if construct
            query = get_query_steps[step]()
            if self.run_arguments["debug"]:
                self.write_compilation_output(query, step.replace(" ", "_").lower())
            if execute:
                try:
                    self.default_db.execute(query)
                except Exception as e:
                    return Exc(e)

            return Ok()

        elif step == "Write Query":
            return self.write_compilation_output(self.sql_query, "select")

        elif step == "Execute Query":
            if execute:
                self.default_db.execute(self.sql_query)

            return Ok()

        elif step == "Cleanup":
            return self.cleanup(self.tmp_table, self.tmp_schema, step, execute)

        elif step == "Cleanup Target":
            return self.cleanup(self.table, self.schema, step, execute)

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

    def create_select(self, table, schema, select, ddl):
        out_sql = list()

        if len(ddl.get("columns")) == 0:
            out_sql.append(
                self.default_db.create_table_select(
                    table, schema, select, view=False, ddl=self.ddl
                )
            )
        else:
            # create table with DDL and insert the output of the select
            out_sql.append(self.default_db.create_table_ddl(table, schema, ddl))

            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            out_sql.append(
                self.default_db.insert(table, schema, select, columns=ddl_column_names)
            )

        return "\n".join(out_sql)

    def cleanup(self, table, schema, step, execute):
        out_sql = list()

        try:
            out_sql.append(
                self.default_db.drop_table(table, schema, view=False, execute=execute)
            )
        except:
            pass

        try:
            out_sql.append(
                self.default_db.drop_table(table, schema, view=True, execute=execute)
            )
        except:
            pass

        query = "\n".join(out_sql)
        if self.run_arguments["debug"]:
            self.write_compilation_output(query, step.replace(" ", "_").lower())

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
        # Get the incremental value

        last_incremental_value_query = (
            f"SELECT MAX({incremental_key}) AS value\n"
            f"FROM {'' if schema is None else schema +'.'}{table}\n"
            f"WHERE {incremental_key} IS NOT NULL"
        )
        if self.run_arguments["debug"]:
            self.write_compilation_output(
                last_incremental_value_query, "last_incremental_value"
            )

        get_data_query = select([source_table_def.c[c["name"]] for c in ddl["columns"]])
        last_incremental_value = None

        if not self.is_full_load and self.target_table_exists:
            if execute:
                res = self.default_db.select(last_incremental_value_query)
                if len(res) == 1:
                    last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = "LAST_INCREMENTAL_VALUE"

        # Select stream
        if last_incremental_value is not None:
            get_data_query = get_data_query.where(
                or_(
                    source_table_def.c[incremental_key].is_(None),
                    source_table_def.c[incremental_key] > last_incremental_value,
                )
            )
        if self.run_arguments["debug"]:
            self.write_compilation_output(get_data_query, "get_data")

        if execute:
            data_iter = source_db.select_stream(get_data_query)
            return self.default_db.load_data(tmp_table, data_iter, schema=tmp_schema)
        else:
            return Ok()
