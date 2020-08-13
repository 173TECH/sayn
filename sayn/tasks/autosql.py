from pathlib import Path
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, FilePath, validator

from ..core.errors import ConfigError, TaskCreationError
from .sql import SqlTask


class Destination(BaseModel):
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    _db_properties: List
    _db_type: str

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None:
            raise ConfigError(
                f'tmp_schema not supported for database of type {v["_db_type"]}'
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
            raise ConfigError(
                'Accepted materialisations: "table", "view" and "incremental".'
            )
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ConfigError("delete_key is not valid for non-incremental loads.")
        elif v == "incremental" and values.get("delete_key") is None:
            raise ConfigError("delete_key is required for incremental materialisation.")
        else:
            return v


class AutoSqlTask(SqlTask):
    def setup(self, **kwargs):
        # TODO control this better
        kwargs["destination"].update(
            {
                "_db_features": self.default_db.sql_features,
                "_db_type": self.default_db.db_type,
            }
        )
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"], **kwargs)
        self.materialisation = self.config.materialisation
        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"
        self.delete_key = self.config.delete_key
        self.ddl = self.default_db.validate_ddl(self.config.ddl)
        self.template = self.get_template(self.config.file_name)

        self.setup_sql()

        return self.ready()

    def run(self):
        self.compiled = self.get_compiled_query()
        return super().run()

    def compile(self):
        self.compiled = self.get_compiled_query()
        return super().compile()

    def get_compiled_query(self):
        if self.materialisation == "view":
            compiled = self.create_query
        # table | incremental when table does not exit or full load
        elif self.materialisation == "table" or (
            self.materialisation == "incremental"
            and (
                not self.default_db.table_exists(self.table, self.schema)
                or self.project_config["full_load"]
            )
        ):
            compiled = f"{self.create_query}\n\n-- Move table\n{self.move_query}"
        # incremental in remaining cases
        else:
            compiled = f"{self.create_query}\n\n-- Merge table\n{self.merge_query}"

        if self.permissions_query is not None:
            compiled += f"\n-- Grant permissions\n{self.permissions_query}"

        return compiled

    # Task property methods

    def setup_sql(self):
        # Autosql tasks are split in 4 queries depending on the materialisation
        # 1. Create: query to create and load data
        self.create_query = None
        # 2. Move: query to moves the tmp table to the target one
        self.move_query = None
        # 3. Merge: query to merge data into the target object
        self.merge_query = None
        # 4. Permissions: grants permissions if necessary
        self.permissions_query = None

        # 0. Retrieve the select statement compiled with jinja
        try:
            query = self.compile_obj(self.template)
        except Exception as e:
            raise TaskCreationError(f"Error compiling template\n{e}")

        # 1. Create query
        if self.materialisation == "view":
            # Views just replace the current object if it exists
            self.create_query = self.default_db.create_table_select(
                self.table, self.schema, query, replace=True, view=True
            )
        else:
            # We always load data into a temporary table
            if self.ddl.get("columns") is not None:
                # create table with DDL and insert the output of the select
                create_table_ddl = self.default_db.create_table_ddl(
                    self.tmp_table, self.tmp_schema, self.ddl, replace=True
                )
                insert = self.default_db.insert(self.table, self.schema, query)
                create_load = (
                    f"-- Create table\n"
                    f"{create_table_ddl}\n\n"
                    f"-- Load data\n"
                    f"{insert}\n\n"
                )

            else:
                # or create from the select if DDL not present
                create_load = self.default_db.create_table_select(
                    self.tmp_table, self.tmp_schema, query, replace=True, ddl=self.ddl
                )

            # Add indexes if necessary
            if self.ddl.get("indexes") is not None:
                indexes = "\n-- Create indexes\n" + self.default_db.create_indexes(
                    self.tmp_table, self.tmp_schema, self.ddl
                )
            else:
                indexes = ""

            self.create_query = create_load + "\n" + indexes

        # 2. Move
        self.move_query = self.default_db.move_table(
            self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl
        )

        # 3. Merge
        if self.materialisation == "incremental":
            self.merge_query = self.default_db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            )

        # Permissions are always the same
        if self.ddl.get("permissions") is not None:
            self.permissions_query = self.default_db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"]
            )
