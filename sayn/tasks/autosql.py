from pathlib import Path
from typing import Dict, Any, Optional

from pydantic import BaseModel, Field, FilePath, validator

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
    ddl: Optional[Dict[str, Any]]

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


class AutoSqlTask(SqlTask):
    def setup(self, **config):
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
            self.config = Config(
                sql_folder=self.run_arguments["folders"]["sql"], **config
            )
        except Exception as e:
            return Exc(e)

        self.tmp_schema = (
            self.config.destination.tmp_schema or self.config.destination.db_schema
        )
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.use_db_object(self.table, schema=self.schema, tmp_schema=self.tmp_schema)

        self.materialisation = self.config.materialisation
        self.delete_key = self.config.delete_key

        # DDL validation
        result = self.target_db._validate_ddl(self.config.ddl)
        if result.is_err:
            return result
        else:
            self.ddl = result.value

        # If we have columns with no type, we can't issue a create table ddl
        # and will issue a create table as select instead.
        # However, if the db doesn't support alter idx then we can't have a
        # primary key
        self.cols_no_type = [c for c in self.ddl["columns"] if c["type"] is None]
        if (
            len(self.ddl["primary_key"]) > 0
            and self.target_db.feature("CANNOT ALTER INDEXES")
            and (len(self.ddl["columns"]) == 0 or len(self.cols_no_type) > 0)
        ):
            return Err(
                "task_definition",
                "missing_column_types_pk",
                columns=self.cols_no_type,
            )

        # Template compilation
        result = self.get_template(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.template = result.value

        result = self.compile_obj(self.template)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        return Ok()

    def execute(self, execute, debug):
        res = self.write_compilation_output(self.sql_query, "select")
        if res.is_err:
            return res

        if self.materialisation == "view":
            # View
            step_queries = self.target_db.replace_view(
                self.table, self.sql_query, schema=self.schema, **self.ddl
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
