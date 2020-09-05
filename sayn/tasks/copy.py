from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

from .sql import SqlTask


class Source(BaseModel):
    db: str
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    _db_properties: List
    _db_type: str


class Destination(BaseModel):
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    _db_properties: List
    _db_type: str

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None:
            raise ValueError(
                f'tmp_schema not supported for database of type {v["_db_type"]}'
            )

        return v


class Config(BaseModel):
    source: Source
    destination: Destination
    ddl: Optional[Dict[str, Any]]
    incremental_key: Optional[str]
    delete_key: Optional[str]

    @validator("incremental_key", always=True)
    def incremental_validation(cls, v, values):
        if (v is None) != (values.get("delete_key") is None):
            raise ValueError(
                'Incremental copy requires both "delete_key" and "incremental_key"'
            )


class CopyTask(SqlTask):
    def setup(self, **config):
        config["source"].update(
            {
                "_db_features": self.default_db.sql_features,
                "_db_type": self.default_db.db_type,
            }
        )
        config["destination"].update(
            {
                "_db_features": self.default_db.sql_features,
                "_db_type": self.default_db.db_type,
            }
        )
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"], **config)

        self.source_db = self.connections[self.config.source.db]
        self.source_schema = self.config.source.db_schema
        self.source_table = self.config.source.table

        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table

        self.delete_key = self.config.delete_key
        self.incremental_key = self.config.incremental_key
        self.ddl = self.default_db.validate_ddl(self.config.ddl)

        result = self.source_db.get_table(
            self.source_table,
            self.source_schema,
            # columns=[c["name"] for c in self.ddl["columns"]],
            # required_existing=True,
        )
        if result.is_err:
            return self.failed(result)

        self.source_table_def = result.value

        # Fill up column types from the source table
        for column in self.ddl["columns"]:
            if column.get("name") not in self.source_table_def.columns:
                return self.failed(
                    "database_error",
                    "source_table_missing_column",
                    {
                        "db": self.source_db.name,
                        "table": self.source_table,
                        "schema": self.source_schema,
                        "column": column.get("name"),
                    },
                )

            # TODO check for incorrect column types
            if "type" not in column:
                column["type"] = self.source_table_def.columns[
                    column["name"]
                ].type.compile(dialect=self.default_db.engine.dialect)

        return self.ready()

    def run(self):
        steps = ["drop_tmp", "create_tmp", "create_indexes", "load_data"]
        if self.default_db.table_exists(self.table, self.schema):
            steps.extend(["merge", "drop_tmp"])
        else:
            steps.extend(["move", "set_permissions"])

        return self.execute_steps(steps)

    def compile(self):
        return self.success()
