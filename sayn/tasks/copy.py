from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

from ..core.errors import Err, Exc, Ok
from ..database import Database
from .base_sql import BaseSqlTask


class Source(BaseModel):
    db_features: List[str]
    db_type: str
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    db: str

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and "NO SET SCHEMA" in values["db_features"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class Destination(BaseModel):
    db_features: List[str]
    db_type: str
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    db: Optional[str]

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and "NO SET SCHEMA" in values["db_features"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
            )

        return v

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and "NO SET SCHEMA" in values["db_features"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class Config(BaseModel):
    source: Source
    destination: Destination
    ddl: Optional[Dict[str, Any]]
    delete_key: Optional[str]
    incremental_key: Optional[str]

    @validator("incremental_key", always=True)
    def incremental_validation(cls, v, values):
        if (v is None) != (values.get("delete_key") is None):
            raise ValueError(
                'Incremental copy requires both "delete_key" and "incremental_key"'
            )

        return v


class CopyTask(BaseSqlTask):
    def setup(self, **config):
        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        # check the source db exists in settings
        if (
            isinstance(config.get("source"), dict)
            and config["source"].get("db") is not None
        ):
            if config["source"]["db"] not in conn_names_list:
                return Err(
                    "task_definition",
                    "source_db_not_in_settings",
                    db=config["source"]["db"],
                )

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

        if isinstance(config.get("source"), dict):
            config["source"].update(
                {
                    "db_features": self.connections[
                        config["source"]["db"]
                    ].sql_features,
                    "db_type": self.connections[config["source"]["db"]].db_type,
                }
            )

        if isinstance(config.get("destination"), dict):
            config["destination"].update(
                {
                    "db_features": self.target_db.sql_features,
                    "db_type": self.target_db.db_type,
                }
            )

        try:
            self.config = Config(**config)
        except Exception as e:
            return Exc(e)

        self.source_db = self.connections[self.config.source.db]
        self.source_schema = self.config.source.db_schema
        self.source_table = self.config.source.table

        self.tmp_schema = (
            self.config.destination.tmp_schema or self.config.destination.db_schema
        )
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"
        self.use_db_object(self.tmp_table, schema=self.tmp_schema)
        self.use_db_object(self.table, schema=self.schema)

        self.delete_key = self.config.delete_key
        self.incremental_key = self.config.incremental_key

        self.is_full_load = self.run_arguments["full_load"] or self.delete_key is None

        result = self.target_db._validate_ddl(self.config.ddl)
        if result.is_ok:
            self.ddl = result.value
        else:
            return result

        result = self.get_columns()
        if result.is_err:
            return result

        # set execution steps
        self.steps = ["Cleanup", "Create Temp DDL"]
        self.steps.append("Load Data")

        if self.is_full_load:
            if len(self.ddl["indexes"]) > 0:
                self.steps.append("Create Indexes")

            self.steps.extend(["Cleanup Target", "Move"])

        else:
            self.steps.extend(["Merge"])

        if len(self.ddl.get("permissions")) > 0:
            self.steps.append("Grant Permissions")

        return Ok()

    def get_columns(self):
        # We get the source table definition
        source_table_def = self.source_db._get_table(
            self.source_table,
            self.source_schema,
            # columns=[c["name"] for c in self.ddl["columns"]],
            # required_existing=True,
        )
        if source_table_def is None:
            return Err(
                "database_error",
                "source_db_missing_source_table",
                schema=self.source_schema,
                table=self.source_table,
                db=self.source_db.name,
            )
        self.source_table_def = source_table_def

        if len(self.ddl["columns"]) == 0:
            dst_table_def = None
            if not self.is_full_load:
                dst_table_def = self.target_db._get_table(self.table, self.schema)

            if dst_table_def is not None:
                # In incremental loads we use the destination table to determine the columns
                self.ddl["columns"] = [
                    {"name": c.name, "type": c.type.compile()}
                    for c in dst_table_def.columns
                ]

                # Ensure these columns are in the source
                missing_columns = set([c.name for c in dst_table_def.columns]) - set(
                    [c.name for c in self.source_table_def.columns]
                )
                if len(missing_columns) > 0:
                    return Err(
                        "database_error",
                        "source_table_missing_columns",
                        db=self.source_db.name,
                        table=self.source_table,
                        schema=self.source_schema,
                        columns=missing_columns,
                    )

            else:
                # In any other case, we use the source
                self.ddl["columns"] = [
                    {
                        "name": c.name,
                        "type": self.source_db._transform_column_type(
                            c.type, self.target_db.engine.dialect
                        ),
                    }
                    for c in self.source_table_def.columns
                ]
        else:
            # Fill up column types from the source table
            for column in self.ddl["columns"]:
                if column.get("name") not in self.source_table_def.columns:
                    return Err(
                        "database_error",
                        "source_table_missing_column",
                        db=self.source_db.name,
                        table=self.source_table,
                        schema=self.source_schema,
                        column=column.get("name"),
                    )

                if "type" not in column:
                    column["type"] = self.source_db._transform_column_type(
                        self.source_table_def.columns[column["name"]].type,
                        self.target_db.engine.dialect,
                    )

        return Ok()

        return Ok()
