import tempfile
import os
import sqlite3
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from sayn.tasks.sql import SqlTask
from sayn.database.creator import create as create_db
from . import inside_dir

sql_query = "CREATE TABLE test_sql_task AS SELECT 1"


# create empty tracker class to enable the run to go through
class VoidTracker:
    def set_run_steps(self, steps):
        pass

    def start_step(self, step):
        pass

    def finish_current_step(self):
        pass


vd = VoidTracker()


def test_sql_task():
    sql_task = SqlTask()
    sql_task.name = "test_sql_task"  # set for compilation output during run
    sql_task.dag = "test_dag"  # set for compilation output during run
    sql_task.run_arguments = {
        "folders": {"sql": "sql", "compile": "compile"},
        "command": "run",
    }
    sql_task.connections = {
        "test_db": create_db(
            "test_db", "test_db", {"type": "sqlite", "database": ":memory:"}
        )
    }
    sql_task._default_db = "test_db"
    sql_task.tracker = vd

    sql_task.jinja_env = Environment(
        loader=FileSystemLoader(os.getcwd()),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )

    tmp_path = tempfile.mkdtemp("sql_task_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "sql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        sql_task.setup("sql_task_test.sql")
        sql_task.run()

    assert sql_task.sql_query == sql_query
    # task_result = conn.execute("SELECT * FROM test_sql_tak")
    # assert task_result[0] == 1
