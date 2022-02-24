from pathlib import Path
from typing import Any, List, Mapping, Optional, Union
from enum import Enum

from pydantic import BaseModel, Field, FilePath, validator, Extra
from colorama import Fore, Style

from ..core.errors import Exc, Ok, Err
from ..database import Database
from .sql import SqlTask


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
    columns: Optional[List[Union[str, Mapping[str, Any]]]] = list()
    table_properties: Mapping[str, Any] = dict()
    post_hook: List[Mapping[str, Any]] = list()

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


class OnFailValue(str, Enum):
    skip = "skip"
    no_skip = "no_skip"


class CompileConfig(BaseModel):
    supports_schemas: bool
    db_type: str

    delete_key: Optional[str]
    materialisation: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: Optional[str]
    columns: Optional[List[Union[str, Mapping[str, Any]]]] = list()
    table_properties: Mapping[str, Any] = dict()
    post_hook: List[Mapping[str, Any]] = list()
    tags: Optional[List[str]]
    parents: Optional[List[str]]
    on_fail: Optional[OnFailValue]

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
        if "task_name" in self._config_input:
            del self._config_input["task_name"]

        # if "columns" in config:
        #     self._has_tests = True

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
            self.ddl = result.value

        if self.run_arguments["command"] == "test" and len(self.ddl["columns"]) != 0:
            result = self.target_db._construct_tests(
                self.ddl["columns"], self.table, self.schema
            )
            if result.is_err:
                return result
            else:
                self.test_query = result.value[0]
                self.test_breakdown = result.value[1]

            if self.test_query is not None:
                self._has_tests = True

        return Ok()

    def config_macro(self, **config):
        if self.allow_config:
            config.update(
                {
                    "supports_schemas": not self.target_db.feature("NO SCHEMA SUPPORT"),
                    "db_type": self.target_db.db_type,
                }
            )
            task_config_override = CompileConfig(**config)

            self.task_config.materialisation = (
                task_config_override.materialisation or self.task_config.materialisation
            )
            self.task_config.destination.db_schema = (
                task_config_override.db_schema or self.task_config.destination.db_schema
            )
            self.task_config.destination.tmp_schema = (
                task_config_override.tmp_schema
                or self.task_config.destination.tmp_schema
            )
            self.task_config.destination.table = (
                task_config_override.table or self.task_config.destination.table
            )
            self.task_config.columns = (
                task_config_override.columns or self.task_config.columns
            )
            self.task_config.table_properties = (
                task_config_override.table_properties
                or self.task_config.table_properties
            )
            self.task_config.post_hook = (
                task_config_override.post_hook or self.task_config.post_hook
            )
            self.task_config.delete_key = (
                task_config_override.delete_key or self.task_config.delete_key
            )
            # Sent to the wrapper
            if task_config_override.on_fail is not None:
                self._config_input["on_fail"] = task_config_override.on_fail

            if task_config_override.tags is not None:
                self._config_input["tags"] = task_config_override.tags

            if task_config_override.parents is not None:
                self._config_input["parents"] = task_config_override.parents

        # Returns an empty string to avoid productin incorrect sql
        return ""

    def setup(self):
        if self.needs_recompile:
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
                **self.ddl,
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
                **self.ddl,
            )

        else:
            # Incremental load
            step_queries = self.target_db.merge_query(
                self.table,
                self.sql_query,
                self.delete_key,
                schema=self.schema,
                tmp_schema=self.tmp_schema,
                **self.ddl,
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
        step_queries = {
            "Write Test Query": self.test_query,
            "Execute Test Query": self.test_query,
        }
        breakdown = self.get_test_breakdown(self.test_breakdown)

        if self.test_query is None:
            self.info("Nothing to be done")
            return self.success()
        else:
            self.set_run_steps(list(step_queries.keys()))

            for step, query in step_queries.items():
                with self.step(step):
                    if "Write" in step:
                        self.write_compilation_output(query, "test")
                    if "Execute" in step:
                        try:
                            result = self.default_db.read_data(query)
                        except Exception as e:
                            return Exc(e)

            if len(result) == 0:
                skipped = [brk for brk in breakdown if brk[0] == "SKIPPED"]
                executed = [brk for brk in breakdown if brk[0] == "EXECUTED"]

                if skipped:
                    self.info(
                        f"{Fore.GREEN}{len(skipped)} test(s) {Style.BRIGHT}SKIPPED{Style.NORMAL}"
                    )
                self.info(
                    f"{Fore.GREEN}{len(executed)} test(s) {Style.BRIGHT}EXECUTED{Style.NORMAL}"
                )

                return self.success()
            else:
                skipped = []
                executed = []
                failed = []
                for brk in breakdown:
                    if any(brk[1] != res["type"] for res in result) or any(
                        brk[2] != res["col"] for res in result
                    ):
                        if brk[0] == "SKIPPED":
                            skipped.append(brk)
                        if brk[0] == "EXECUTED":
                            executed.append(brk)
                    else:
                        failed.append(brk)
                if self.run_arguments["debug"]:

                    fl_info = [f"{Fore.RED}FAILED: "]
                    for info in failed:
                        count = sum(
                            [
                                item["cnt"]
                                for item in result
                                if (item["type"] == info[1] and item["col"] == info[2])
                            ]
                        )
                        values = [
                            item["val"]
                            for item in result
                            if (item["type"] == info[1] and item["col"] == info[2])
                        ]
                        values = ", ".join(values[:5])
                        fl_info.append(
                            f"{Fore.RED}{Style.BRIGHT}{brk[1]} test{Style.NORMAL} on {Style.BRIGHT}{info[2]} FAILED{Style.NORMAL}. {count} offending records. \n\t    Please see some values for which the test failed: {Style.BRIGHT}{values}{Style.NORMAL}"
                        )
                    if skipped:
                        self.info(
                            f"{Fore.GREEN}{len(skipped)} test(s) {Style.BRIGHT}SKIPPED{Style.NORMAL}"
                        )
                    self.info(
                        f"{Fore.GREEN}{len(executed)} test(s) {Style.BRIGHT}EXECUTED{Style.NORMAL}"
                    )
                    for err in fl_info:
                        self.info(err)

                    errinfo = f"Test Failed. You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"
                    return self.fail(errinfo)
                else:
                    summary = f"{len(executed)} tests were ran, {len(executed)-len(failed)} succeeded, "
                    if skipped:
                        summary += f", {len(skipped)} were skipped, "
                    summary += f"{len(failed)} failed."
                    self.warning(summary)
                    errout = ", ".join(list(set([res["type"] for res in result])))
                    return self.fail(f"Failed test types: {errout}")
