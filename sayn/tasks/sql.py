from pathlib import Path

from pydantic import BaseModel, FilePath, validator

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
            self.compiled = self.compile_obj(self.config.file_name)
        except Exception as e:
            return self.fail(message=f"Error compiling template\n{e}")

        self.set_run_steps(["write_query_on_disk", "execute_sql"])

        return self.ready()

    def run(self):
        with self.step("write_query_on_disk"):
            try:
                self.write_compilation_output(self.compiled)
            except Exception as e:
                return self.fail(("Error saving query on disk", f"{e}"))

        with self.step("execute_sql"):
            self.logger.debug(self.compiled)

            try:
                self.default_db.execute(self.compiled)
            except Exception as e:
                return self.fail("Error running query", f"{e}")

        return self.success()

    def compile(self):
        try:
            self.write_compilation_output(self.compiled)
        except Exception as e:
            return self.fail(("Error saving query on disk", f"{e}"))

        return self.success()

    # Task property methods - EXECUTION

    def _exeplan_drop(self, table, schema):
        self.logger.debug(f"Dropping {'' if schema is None else schema +'.'}{table}...")

        try:
            self.default_db.drop_table(table, schema, view=False, execute=True)
        except:
            pass

        try:
            self.default_db.drop_table(table, schema, view=True, execute=True)
        except:
            pass

    def _exeplan_create(self, table, schema, select=None, view=False, ddl=dict()):
        self.logger.debug(
            f"Creating {'view' if view is True else 'table'} {'' if schema is None else schema +'.'}{table}..."
        )
        if ddl.get("columns") is None:
            self.default_db.create_table_select(
                table, schema, select, view=view, execute=True
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

    def _exeplan_create_indexes(self, table, schema, ddl=dict()):
        if ddl.get("indexes") is not None:
            self.logger.debug(
                f"Creating indexes on table {'' if self.schema is None else self.schema +'.'}{self.table}..."
            )
            self.default_db.create_indexes(table, schema, ddl, execute=True)

    def _exeplan_move(self, src_table, src_schema, dst_table, dst_schema, ddl=dict()):
        self.logger.debug(
            "Moving table {src_name} to {dst_name}...".format(
                src_name=f"{'' if src_schema is None else src_schema +'.'}{src_table}",
                dst_name=f"{'' if dst_schema is None else dst_schema +'.'}{dst_table}",
            )
        )
        self.default_db.move_table(
            src_table, src_schema, dst_table, dst_schema, ddl, execute=True
        )

    def _exeplan_merge(self, tmp_table, tmp_schema, table, schema, delete_key):
        self.logger.debug(
            f"Merging into table {'' if schema is None else schema +'.'}{table}..."
        )
        self.default_db.merge_tables(
            tmp_table, tmp_schema, table, schema, delete_key, execute=True
        )

    def _exeplan_set_permissions(self, table, schema, ddl=dict()):
        if ddl.get("permissions") is not None:
            self.logger.debug(
                f"Setting permissions on {'' if schema is None else schema +'.'}{table}..."
            )
            self.default_db.grant_permissions(
                table, schema, ddl["permissions"], execute=True
            )
