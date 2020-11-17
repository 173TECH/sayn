import sqlite3
from pathlib import Path
import pytest

from . import inside_dir, simulate_task
from sayn.core.errors import Result

sql_query = "CREATE TABLE test_sql_task AS SELECT 1 AS x"
sql_query_param = "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x"
sql_query_err = "SELECT * FROM non_existing_table"


def test_sql_task(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query)
        setup_result = task.setup("test.sql")
        run_result = task.run()
        # setup_result, run_result = simulate_task_setup_run(, sql_query=sql_query)

        task_result = task.default_db.select("SELECT * FROM test_sql_task")

        assert task.sql_query == sql_query
        assert task.steps == ["Write Query", "Execute Query"]
        assert task_result[0]["x"] == 1
        assert setup_result.is_ok
        assert run_result.is_ok
