from pathlib import Path

from pydantic import BaseModel, FilePath, validator

from . import Task
from ..core.errors import TaskCreationError


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(Task):
    def setup(self, file_name):
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"])

        try:
            self.compiled = self.compile_obj(self.config.file_name)
        except Exception as e:
            raise TaskCreationError(f"Error compiling template\n{e}")

        return self.ready()

    def run(self):
        self.logger.set_steps(["write_query_on_disk", "execute_sql"])

        self.logger.set_current_step("write_query")

        if self.name == "dim_fighters":
            raise ValueError("adfadf")
        try:
            self.write_compilation_output(self.compiled)
        except Exception as e:
            return self.fail(("Error saving query on disk", f"{e}"))

        self.logger.set_current_step("execute_sql")
        self.logger.debug(self.compiled)

        try:
            self.default_db.execute(self.compiled)
        except Exception as e:
            return self.fail(("Error running query", f"{e}", f"Query: {self.compiled}"))

        return self.success()

    def compile(self):
        self.logger.set_steps(["write_query_on_disk"])
        self.logger.set_current_step("write_query")

        try:
            self.write_compilation_output(self.compiled)
        except Exception as e:
            return self.fail(("Error saving query on disk", f"{e}"))

        return self.success()
