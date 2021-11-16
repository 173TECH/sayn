from pathlib import Path

from pydantic import BaseModel, FilePath, validator, Extra
from typing import Optional

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath
    db: Optional[str]

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class TestTask(Task):
    def setup(self, **config):
        self.debug("Nothing to be done")
        return self.success()

    #
    # def run(self):
    #     self.debug("Nothing to be done")
    #     return self.success()
    #
    # def compile(self):
    #     self.debug("Nothing to be done")
    #     return self.success()

    def test(self):
        self.debug("Nothing to be done")
        return self.success()
