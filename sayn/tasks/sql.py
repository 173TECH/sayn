from pathlib import Path

from pydantic import BaseModel, FilePath, validator

from ..core.errors import Ok
from .base_sql import BaseSqlTask


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(BaseSqlTask):
    def setup(self, file_name):
        self.config = Config(
            sql_folder=self.run_arguments["folders"]["sql"], file_name=file_name
        )

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        # Set execution steps
        self.steps = ["Write Query"]
        if self.run_arguments["command"] == "run":
            self.steps.append("Execute Query")

        return Ok()
