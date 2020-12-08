from pathlib import Path
from typing import Dict, Any, List, Optional

from pydantic import BaseModel, Field, FilePath, validator

from ..core.errors import Exc, Ok, Err
from ..database import Database
from .base_sql import BaseSqlTask


class Destination(BaseModel):
    db_features: List[str]
    db_type: str
    db: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and "NO SET SCHEMA" in values["db_features"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
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


class AutoSqlTask(BaseSqlTask):
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
                    "db_features": self.target_db.sql_features,
                    "db_type": self.target_db.db_type,
                }
            )

        try:
            self.config = Config(
                sql_folder=self.run_arguments["folders"]["sql"], **config
            )
        except Exception as e:
            return Exc(e)

        self.materialisation = self.config.materialisation
        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"
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
        cols_no_type = [c for c in self.ddl["columns"] if c["type"] is None]
        if (
            len(self.ddl["primary_key"]) > 0
            and "NO ALTER INDEXES" in self.target_db.sql_features
        ) and (len(self.ddl["columns"]) == 0 or len(cols_no_type) > 0):
            return Err(
                "task_definition", "missing_column_types_pk", columns=cols_no_type,
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

        # Materialisation type
        self.is_full_load = (
            self.materialisation == "table" or self.run_arguments["full_load"]
        )

        # Sets the execution steps
        self.steps = ["Write Query"]

        if self.materialisation == "view":
            self.steps.extend(["Cleanup Target", "Create View"])

        else:
            self.steps.extend(["Cleanup", "Create Temp"])

            if self.is_full_load:
                if len(self.ddl["indexes"]) > 0 or (
                    len(self.ddl["primary_key"]) > 0
                    and len(self.ddl["columns"]) > 0
                    and len(cols_no_type) > 0
                ):
                    self.steps.append("Create Indexes")
                self.steps.extend(["Cleanup Target", "Move"])

            else:
                self.steps.append("Merge")

        if len(self.ddl.get("permissions")) > 0:
            self.steps.append("Grant Permissions")

        return Ok()
