import tempfile
import os
import sqlite3
from pathlib import Path
import pytest

from . import inside_dir, simulate_sql_task
from sayn.core.errors import Result

sql_query = "CREATE TABLE test_sql_task AS SELECT 1 AS x"
sql_query_param = "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x"
sql_query_err = "SELECT * FROM non_existing_table"


def test_sql_task():
    sql_task = simulate_sql_task()

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "sql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = sql_task.setup("sql_task_test.sql")
        run_result = sql_task.run()

    task_result = sql_task.default_db.select("SELECT * FROM test_sql_task")

    assert sql_task.sql_query == sql_query
    assert sql_task.steps == ["Write Query", "Execute Query"]
    assert task_result[0]["x"] == 1
    assert setup_result.error is None
    assert run_result.error is None


def test_sql_task_param():
    sql_task = simulate_sql_task()
    sql_task.task_parameters = {"user_prefix": "tu_"}
    sql_task.jinja_env.globals.update(**sql_task.task_parameters)

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "sql_task_test_param.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query_param)
        setup_result = sql_task.setup("sql_task_test_param.sql")
        run_result = sql_task.run()

    task_result = sql_task.default_db.select("SELECT * FROM tu_test_sql_task")

    assert sql_task.sql_query == "CREATE TABLE tu_test_sql_task AS SELECT 1 AS x"
    assert task_result[0]["x"] == 1
    assert setup_result.error is None
    assert run_result.error is None


def test_sql_task_param_err():
    sql_task = simulate_sql_task()

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "sql_task_test_param_err.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query_param)
        setup_result = sql_task.setup("sql_task_test_param_err.sql")

    assert setup_result.error is not None


def test_sql_task_run_err():
    sql_task = simulate_sql_task()

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "sql_task_test_run_err.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query_err)
        setup_result = sql_task.setup("sql_task_test_run_err.sql")
        with pytest.raises(sqlite3.OperationalError):
            run_result = sql_task.run()
            assert run_result.error is not None

    assert setup_result.error is None
