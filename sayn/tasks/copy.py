from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

from ..core.errors import Err, Ok
from ..database import Database
from .base_sql import BaseSqlTask


class Source(BaseModel):
    db_features: List[str]
    db_type: str
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    all_dbs: List[str]
    db: str

    @validator("db")
    def source_db_exists(cls, v, values):
        if v not in values["all_dbs"]:
            raise ValueError(f'"{v}" is not a valid database')
        else:
            return v


class Destination(BaseModel):
    db_features: List[str]
    db_type: str
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
        if isinstance(config.get("source"), dict):
            config["source"].update(
                {
                    "db_features": self.default_db.sql_features,
                    "db_type": self.default_db.db_type,
                    "all_dbs": [
                        n
                        for n, c in self.connections.items()
                        if isinstance(c, Database)
                    ],
                }
            )

        if isinstance(config.get("destination"), dict):
            config["destination"].update(
                {
                    "db_features": self.default_db.sql_features,
                    "db_type": self.default_db.db_type,
                }
            )

        self.config = Config(**config)

        self.source_db = self.connections[self.config.source.db]
        self.source_schema = self.config.source.db_schema
        self.source_table = self.config.source.table

        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"

        self.delete_key = self.config.delete_key
        self.incremental_key = self.config.incremental_key

        self.is_full_load = self.run_arguments["full_load"] or self.delete_key is None

        result = self.default_db._validate_ddl(self.config.ddl)
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
        result = self.source_db.get_table(
            self.source_table,
            self.source_schema,
            # columns=[c["name"] for c in self.ddl["columns"]],
            # required_existing=True,
        )
        if result.is_err:
            return result
        self.source_table_def = result.value

        if len(self.ddl["columns"]) == 0:
            dst_table_def = None
            if not self.is_full_load:
                result = self.default_db.get_table(self.table, self.schema)
                if result.is_ok:
                    dst_table_def = result.value

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
                            c.type, self.default_db.engine.dialect
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
                        self.default_db.engine.dialect,
                    )

        return Ok()
