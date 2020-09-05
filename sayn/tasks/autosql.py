from pathlib import Path
from typing import Dict, Any, List, Optional

from pydantic import BaseModel, Field, FilePath, validator

from ..core.errors import SaynValidationError, Exc, Ok
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
            raise SaynValidationError("tmp_schema_not_supported", v["_db_type"])

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
            raise SaynValidationError("invalidid_materialisation", v)
        elif v != "incremental" and values.get("delete_key") is not None:
            raise SaynValidationError("incremental_spec_error", "delete_key")
        elif v == "incremental" and values.get("delete_key") is None:
            raise SaynValidationError("non_incremental_spec_error", "delete_key")
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

        result = self.default_db.validate_ddl(self.config.ddl)
        if result.is_err:
            return result
        else:
            self.ddl = result.value

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
