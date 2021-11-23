from pathlib import Path
import json

from pydantic import BaseModel, Field, FilePath, validator, Extra
from typing import List, Optional, Union

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Destination(BaseModel):
    supports_schemas: bool
    db_type: str
    db: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str

    class Config:
        extra = Extra.forbid

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


class Tests(BaseModel):
    name: str
    values: Optional[List[str]]

    class Config:
        extra = Extra.forbid


class Columns(BaseModel):
    name: str
    description: Optional[str]
    tests: List[Union[str, Tests]]

    class Config:
        extra = Extra.forbid


class Config(BaseModel):
    columns: List[Columns]
    destination: Destination

    class Config:
        extra = Extra.forbid


class TestTask(Task):
    def setup(self, **config):
        return self.success()

    def test(self):
        with self.step("Write Test Query"):
            result = self.write_compilation_output(self.query, "test")
            if result.is_err:
                return result

        with self.step("Execute Test Query"):
            result = self.default_db.read_data(self.query)

            if len(result) == 0:
                return self.success()
            else:
                errout = "Test failed, problematic fields:\n"
                for res in result:
                    errout += json.dumps(res)
                return self.fail(errout)
