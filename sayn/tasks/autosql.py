from pathlib import Path
from typing import Dict, Any, Optional, List

from pydantic import BaseModel, Field, FilePath, validator, Extra
from terminaltables import AsciiTable

from ..core.errors import Exc, Ok, Err
from ..database import Database
from .sql import SqlTask

# from .test import Columns


class Destination(BaseModel):
    supports_schemas: bool
    db_type: str
    db: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str

    class Config:
        extra = Extra.forbid

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
            )

        return v

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath
    delete_key: Optional[str]
    materialisation: str
    destination: Destination
    # ddl: Optional[Dict[str, Any]]
    columns: Optional[List[Dict[str, Any]]] = list()
    table_properties: Optional[List[Dict[str, Any]]] = list()
    post_hook: Optional[List[Dict[str, Any]]] = list()

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)

    @validator("materialisation")
    def incremental_has_delete_key(cls, v, values):
        if v not in ("table", "view", "incremental"):
            raise ValueError(f'"{v}". Valid materialisations: table, view, incremental')
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ValueError('"delete_key" is invalid in non-incremental loads')
        elif v == "incremental" and values.get("delete_key") is None:
            raise ValueError('"delete_key" is required for incremental loads')
        else:
            return v


class CompileConfig(BaseModel):
    supports_schemas: bool
    db_type: str

    delete_key: Optional[str]
    materialisation: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: Optional[str]
    # ddl: Optional[Dict[str, Any]]
    columns: Optional[List[Dict[str, Any]]] = list()
    table_properties: Optional[List[Dict[str, Any]]] = list()
    post_hook: Optional[List[Dict[str, Any]]] = list()

    class Config:
        extra = Extra.forbid

    @validator("materialisation")
    def incremental_has_delete_key(cls, v, values):
        if v not in ("table", "view", "incremental"):
            raise ValueError(f'"{v}". Valid materialisations: table, view, incremental')
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ValueError('"delete_key" is invalid in non-incremental loads')
        elif v == "incremental" and values.get("delete_key") is None:
            raise ValueError('"delete_key" is required for incremental loads')
        else:
            return v

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
            )

        return v

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class AutoSqlTask(SqlTask):
    def config(self, **config):
        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        # set the target db for execution
        # this check needs to happen here so we can pass db_features and db_type to the validator
        if (
            isinstance(config.get("destination"), dict)
            and config["destination"].get("db") is not None
        ):
            if config["destination"]["db"] not in conn_names_list:
                return Err(
                    "task_definition",
                    "destination_db_not_in_settings",
                    db=config["destination"]["db"],
                )
            self._target_db = config["destination"]["db"]
        else:
            self._target_db = self._default_db

        if isinstance(config.get("destination"), dict):
            config["destination"].update(
                {
                    "supports_schemas": not self.target_db.feature("NO SCHEMA SUPPORT"),
                    "db_type": self.target_db.db_type,
                }
            )

        try:
            self.task_config = Config(
                sql_folder=self.run_arguments["folders"]["sql"], **config
            )
        except Exception as e:
            return Exc(e)

        # We compile first to allow changes to the config
        # Template compilation
        self.compiler.update_globals(
            src=lambda x: self.src(x, connection=self._target_db),
            config=self.config_macro,
        )

        self.allow_config = True

        self.prepared_sql_query = self.compiler.prepare(self.task_config.file_name)
        self.sql_query = self.prepared_sql_query.compile()

        # After the first compilation, config shouldn't be executed again
        self.allow_config = False

        # Output calculation
        db_schema = self.task_config.destination.db_schema

        if self.task_config.destination.tmp_schema is not None:
            tmp_db_schema = self.task_config.destination.tmp_schema
        else:
            tmp_db_schema = db_schema

        base_table_name = self.task_config.destination.table

        if db_schema is None:
            self.schema = None
            self.table = self.out(base_table_name, self.target_db)
        else:
            obj = self.out(f"{db_schema}.{base_table_name}", self.target_db)
            self.schema = obj.split(".")[0]
            self.table = obj.split(".")[1]

        if tmp_db_schema is None:
            self.tmp_schema = None
            self.tmp_table = self.out(f"sayn_tmp_{base_table_name}", self.target_db)
        else:
            obj = self.out(
                f"{tmp_db_schema}.sayn_tmp_{base_table_name}", self.target_db
            )
            self.tmp_schema = obj.split(".")[0]
            self.tmp_table = obj.split(".")[1]

        self.materialisation = self.task_config.materialisation
        self.delete_key = self.task_config.delete_key

        # DDL validation
        result = self.target_db._validate_ddl(
            self.task_config.columns,
            self.task_config.table_properties,
            self.task_config.post_hook,
        )
        if result.is_err:
            return result
        else:
            self.columns = result.value

        if self.run_arguments["command"] == "test":
            result = self.target_db._construct_tests(
                self.columns["columns"], self.table, self.schema
            )
            if result.is_err:
                return result
            else:
                self.test_query = result.value[0]
                self.test_breakdown = result.value[1]

        return Ok()

    def config_macro(self, **config):
        if self.allow_config:
            config.update(
                {
                    "supports_schemas": not self.target_db.feature("NO SCHEMA SUPPORT"),
                    "db_type": self.target_db.db_type,
                }
            )
            task_config_overload = CompileConfig(**config)
            self.task_config.materialisation = (
                task_config_overload.materialisation or self.task_config.materialisation
            )
            self.task_config.destination.db_schema = (
                task_config_overload.db_schema or self.task_config.destination.db_schema
            )
            self.task_config.destination.tmp_schema = (
                task_config_overload.tmp_schema
                or self.task_config.destination.tmp_schema
            )
            self.task_config.destination.table = (
                task_config_overload.table or self.task_config.destination.table
            )
            self.task_config.columns = (
                task_config_overload.columns or self.task_config.columns
            )
            self.task_config.table_properties = (
                task_config_overload.table_properties
                or self.task_config.table_properties
            )
            self.task_config.post_hook = (
                task_config_overload.post_hook or self.task_config.post_hook
            )
            self.task_config.delete_key = (
                task_config_overload.delete_key or self.task_config.delete_key
            )

        # Returns an empty string to avoid productin incorrect sql
        return ""

    def setup(self, needs_recompile):
        if needs_recompile:
            self.sql_query = self.prepared_sql_query.compile()

        return Ok()

    def execute(self, execute, debug):
        if self.run_arguments["debug"]:
            self.write_compilation_output(self.sql_query, "select")
        else:
            self.write_compilation_output(self.sql_query)

        if self.materialisation == "view":
            # View
            step_queries = self.target_db.replace_view(
                self.table,
                self.sql_query,
                schema=self.schema,
                **self.columns,
            )

        elif (
            self.materialisation == "table"
            or self.run_arguments["full_load"]
            or self.target_db._requested_objects[self.schema][self.table].get("type")
            is None
        ):
            # Full load or target table missing
            if self.target_db.feature("CANNOT CHANGE SCHEMA"):
                # Use destination schema if the db doesn't support schema changes
                tmp_schema = self.schema
            else:
                tmp_schema = self.tmp_schema

            step_queries = self.target_db.replace_table(
                self.table,
                self.sql_query,
                schema=self.schema,
                tmp_schema=tmp_schema,
                **self.columns,
            )

        else:
            # Incremental load
            step_queries = self.target_db.merge_query(
                self.table,
                self.sql_query,
                self.delete_key,
                schema=self.schema,
                tmp_schema=self.tmp_schema,
                **self.columns,
            )

        self.set_run_steps(list(step_queries.keys()))

        for step, query in step_queries.items():
            with self.step(step):
                if debug:
                    self.write_compilation_output(query, step.replace(" ", "_").lower())
                if execute:
                    try:
                        self.target_db.execute(query)
                    except Exception as e:
                        return Exc(e)

        return Ok()

    def compile(self):
        return self.execute(False, self.run_arguments["debug"])

    def run(self):
        return self.execute(True, self.run_arguments["debug"])

    def test(self):
        if self.test_query == "":
            self.info(self.get_test_breakdown(self.test_breakdown))
            return self.success()
        else:
            with self.step("Write Test Query"):
                self.write_compilation_output(self.test_query, "test")

            with self.step("Execute Test Query"):
                result = self.default_db.read_data(self.test_query)

                self.info(self.get_test_breakdown(self.test_breakdown))

                if len(result) == 0:
                    return self.success()
                else:
                    errout = "Test failed, summary:\n\n"
                    errout += f"Total number of offending records: {sum([item['cnt'] for item in result])} \n"
                    data = []
                    data.append(
                        ["Breach Count", "Prob. Value", "Test Type", "Failed Fields"]
                    )

                    for res in result:
                        data.append([res["cnt"], res["val"], res["type"], res["col"]])
                    table = AsciiTable(data)

                    errinfo = f"You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"

                    return self.fail(errout + table.table + "\n\n" + errinfo)
