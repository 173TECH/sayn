from pathlib import Path
from typing import Dict, Any, List, Optional

from pydantic import BaseModel, Field, FilePath, validator

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
            raise ValueError(
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
            raise ValueError(
                'Accepted materialisations: "table", "view" and "incremental".'
            )
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ValueError("delete_key is not valid for non-incremental loads.")
        elif v == "incremental" and values.get("delete_key") is None:
            raise ValueError("delete_key is required for incremental materialisation.")
        else:
            return v


class AutoSqlTask(SqlTask):
    def setup(self, **config):
        # TODO control this better
        config["destination"].update(
            {
                "_db_features": self.default_db.sql_features,
                "_db_type": self.default_db.db_type,
            }
        )
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"], **config)
        self.materialisation = self.config.materialisation
        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"
        self.delete_key = self.config.delete_key
        self.ddl = self.default_db.validate_ddl(self.config.ddl)
        self.template = self.get_template(self.config.file_name)

        try:
            self.sql_query = self.compile_obj(self.template)
        except Exception as e:
            return self.fail(message=f"Error compiling template\n{e}")

        return self.ready()

    def run(self):
        steps = ["write_query_on_disk"]

        if self.materialisation == "view":  # View
            steps.extend(["drop", "create_view"])

        elif (
            self.materialisation == "incremental"
            and not self.run_arguments["full_load"]
            and self.default_db.table_exists(self.table, self.schema)
        ):  # Incremental load
            steps.extend(["drop_tmp", "create_tmp_ddl", "merge", "drop_tmp"])

        else:  # Full load
            steps.extend(
                ["drop_tmp", "create_tmp_ddl", "create_indexes", "drop", "move"]
            )

        steps.append("set_permissions")

        return self.execute_steps(steps)
