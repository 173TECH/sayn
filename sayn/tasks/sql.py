from pathlib import Path
from typing import Any, List, Mapping, Optional, Union
from enum import Enum
import re

from pydantic import BaseModel, FilePath, validator, Extra

from ..core.errors import Exc, Ok, Err
from ..database import Database
from .task import Task


class BaseConfig(BaseModel):
    supports_schemas: bool
    db_type: str

    delete_key: Optional[str]
    destination: Optional[str]
    materialisation: Optional[str]
    db: Optional[str]
    tmp_schema: Optional[str]

    columns: Optional[List[Union[str, Mapping[str, Any]]]] = list()
    table_properties: Mapping[str, Any] = dict()
    post_hook: List[Mapping[str, Any]] = list()

    class Config:
        extra = Extra.forbid


class Config(BaseConfig):
    sql_folder: Path
    file_name: FilePath

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
            )

        return v

    @validator("materialisation")
    def incremental_has_delete_key(cls, v, values):
        if v not in ("table", "view", "incremental", "script", ""):
            raise ValueError(
                f'"{v}". Valid materialisations: table, view, incremental, script'
            )
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ValueError('"delete_key" is invalid in non-incremental loads')
        elif v == "incremental" and values.get("delete_key") is None:
            raise ValueError('"delete_key" is required for incremental loads')
        elif v != "script" and values.get("destination") is None:
            raise ValueError('"destination" field is required')
        elif v == "script" and values.get("destination") is not None:
            raise ValueError('"destination" is invalid for script materialisation')
        elif v == "":
            return "script"
        else:
            return v

    @validator("destination")
    def can_use_schema(cls, v, values):
        match = re.match(r"(.*)\.(.*)|(\w+)", v)
        if match.group(1) is not None and not values["supports_schemas"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class OnFailValue(str, Enum):
    skip = "skip"
    no_skip = "no_skip"


class CompileConfig(BaseConfig):
    tags: Optional[List[str]]
    sources: Optional[List[str]]
    outputs: Optional[List[str]]
    parents: Optional[List[str]]
    on_fail: Optional[OnFailValue]

    class Config:
        extra = Extra.forbid


class SqlTask(Task):
    @property
    def target_db(self):
        return self.connections[self._target_db]

    def config(self, **config):

        def_connections_src = []
        def_connections_out = []

        def def_src(obj, level=None):
            def_connections_src.append((obj, level))
            return ""

        def def_out(obj, level=None):
            def_connections_out.append((obj, level))
            return ""

        if "task_name" in self._config_input:
            del self._config_input["task_name"]

        # if "columns" in config:
        #     self._has_tests = True

        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        # set the target db for execution
        # this check needs to happen here so we can pass db_features and db_type to the validator
        if isinstance(config.get("db"), str):
            if config["db"] not in conn_names_list:
                return Err(
                    "task_definition",
                    "destination_db_not_in_settings",
                    db=config["db"],
                )
            self._target_db = config["db"]
        else:
            self._target_db = self._default_db

        config.update(
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
            src=lambda x, level=None: def_src(x),
            out=lambda x, level=None: def_out(x),
            config=self.config_macro,
        )
        self.allow_config = True

        self.prepared_sql_query = self.compiler.prepare(self.task_config.file_name)
        self.sql_query = self.prepared_sql_query.compile()

        # After the first compilation, config shouldn't be executed again
        self.allow_config = False

        if isinstance(config.get("destination"), str):
            match = re.match(r"(.*)\.(.*)|(\w+)", self.task_config.destination)

            if match.group(3):
                db_schema = None
                base_table_name = match.group(3)
            else:
                db_schema = match.group(1)
                base_table_name = match.group(2)
            # Output calculation
            if self.task_config.tmp_schema is not None:
                tmp_db_schema = self.task_config.tmp_schema
            else:
                tmp_db_schema = db_schema

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

        self.compiler.update_globals(
            src=lambda x, level=None: self.src(
                x, connection=self._target_db, level=level
            ),
            out=lambda x, level=None: self.out(
                x, connection=self._target_db, level=level
            ),
            config=self.config_macro,
        )

        for s in def_connections_src:
            self.src(s[0], connection=self._target_db, level=s[1])

        for o in def_connections_out:
            self.out(o[0], connection=self._target_db, level=o[1])

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

            self.task_config.db = (
                task_config_override.db or self.task_config.db or self._default_db
            )

            conn_names_list = [
                n for n, c in self.connections.items() if isinstance(c, Database)
            ]

            if self.task_config.db not in conn_names_list:
                return Err(
                    "task_definition",
                    "destination_db_not_in_settings",
                    db=config["db"],
                )
            else:
                self._target_db = self.task_config.db

            self.task_config.tmp_schema = (
                task_config_override.tmp_schema or self.task_config.tmp_schema
            )

            self.task_config.destination = (
                task_config_override.destination or self.task_config.destination
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

            if task_config_override.sources is not None:
                self._config_input["sources"] = task_config_override.sources

            if task_config_override.outputs is not None:
                self._config_input["outputs"] = task_config_override.outputs

            if task_config_override.parents is not None:
                self._config_input["parents"] = task_config_override.parents
        # Returns an empty string to avoid productin incorrect sql
        return ""

    def setup(self):
        # recompile regardless
        self.sql_query = self.prepared_sql_query.compile()

        if self._has_tests and self._needs_recompile:
            obj = self.src(f"{self.task_config.destination}", self.target_db)
            test_schema = obj.split(".")[0]
            test_table = obj.split(".")[1]
            result = self.target_db._construct_tests(
                self.ddl["columns"], test_table, test_schema
            )
            if result.is_err:
                return result
            else:
                self.test_query = result.value[0]
                self.test_breakdown = result.value[1]

        return Ok()

    def execute(self, execute, debug):
        if self.materialisation in ("table", "view", "incremental"):
            if self.run_arguments["debug"]:
                self.write_compilation_output(self.sql_query, "select")
            else:
                self.write_compilation_output(self.sql_query)
        elif self.materialisation in ("script",):
            self.write_compilation_output(self.sql_query)

        if self.materialisation == "view":
            # View
            step_queries = self.target_db.replace_view(
                self.table,
                self.sql_query,
                schema=self.schema,
                **self.ddl,
            )

        elif self.materialisation == "table" or self.run_arguments["full_load"]:
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

        elif self.materialisation == "incremental":
            # Incremental load
            step_queries = self.target_db.merge_query(
                self.table,
                self.sql_query,
                self.delete_key,
                schema=self.schema,
                tmp_schema=self.tmp_schema,
                **self.ddl,
            )
        else:
            # script
            step_queries = {"Write Query": None, "Execute Query": self.sql_query}

        self.set_run_steps(list(step_queries.keys()))

        for step, query in step_queries.items():
            with self.step(step):
                if debug and query:
                    self.write_compilation_output(query, step.replace(" ", "_").lower())
                if execute and query:
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
                return self.test_sucessful(breakdown)
            else:
                errout, failed = self.test_failure(
                    breakdown, result, self.run_arguments["debug"]
                )
                problematic_values_query = self.default_db.test_problematic_values(
                    failed, self.table, self.schema
                )

                for query in problematic_values_query.split(";"):
                    if query.strip():
                        header = re.search(r"--.*?--", query).group(0)
                        self.info("")
                        self.info(header)
                        self.info(
                            "===================================================================="
                        )
                        self.info(
                            re.sub(r"--.*?--", "", query).replace("\n", " ").strip()
                            + ";"
                        )
                        self.info(
                            "===================================================================="
                        )
                        self.info("")

                self.write_compilation_output(
                    problematic_values_query, "test_problematic_values"
                )

                return errout
