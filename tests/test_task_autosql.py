import tempfile
import sqlite3
from pathlib import Path
import pytest

from . import inside_dir, simulate_task, simulate_task_setup, simulate_task_setup_run
from sayn.core.errors import Result

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_err = "SELECTS * FROM non_existing_table"


def test_autosql_task_table():
    task = simulate_task("autosql")
    task.config = {
        "file_name": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }
    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query, task_config=task.config
    )

    task_result = task.default_db.select("SELECT * FROM test_autosql_task")
    task_table = task.default_db.select(
        'SELECT * FROM sqlite_master WHERE type="table" AND NAME = "test_autosql_task"'
    )

    assert task.sql_query == sql_query
    assert task.steps == [
        "Write Query",
        "Cleanup",
        "Create Temp",
        "Cleanup Target",
        "Move",
    ]
    assert task_result[0]["x"] == 1
    assert len(task_table) == 1
    assert setup_result.is_ok
    assert run_result.is_ok


def test_autosql_task_view():
    task = simulate_task("autosql")
    task.config = {
        "file_name": "test.sql",
        "materialisation": "view",
        "destination": {"table": "test_autosql_task"},
    }
    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query, task_config=task.config
    )

    task_result = task.default_db.select("SELECT * FROM test_autosql_task")
    task_table = task.default_db.select(
        'SELECT * FROM sqlite_master WHERE type="view" AND NAME = "test_autosql_task"'
    )

    assert task.sql_query == sql_query
    assert task.steps == ["Write Query", "Cleanup Target", "Create View"]
    assert task_result[0]["x"] == 1
    assert len(task_table) == 1
    assert setup_result.is_ok
    assert run_result.is_ok


def test_autosql_task_compile():
    task = simulate_task("autosql")
    task.run_arguments.update({"command": "compile"})
    task.config = {
        "file_name": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }
    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query, task_config=task.config
    )

    assert task.sql_query == sql_query
    assert task.steps == [
        "Write Query",
        "Cleanup",
        "Create Temp",
        "Cleanup Target",
        "Move",
    ]
    assert setup_result.is_ok
    assert run_result.is_ok


def test_autosql_task_param():
    task = simulate_task("autosql")
    task.task_parameters = {"number": 1}
    task.jinja_env.globals.update(**task.task_parameters)
    task.config = {
        "file_name": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query_param, task_config=task.config
    )

    task_result = task.default_db.select("SELECT * FROM test_autosql_task")

    assert task_result[0]["x"] == 1
    assert setup_result.is_ok
    assert run_result.is_ok


def test_autosql_task_config_error1():
    task = simulate_task("autosql")
    task.config = {
        "file_nam": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    setup_result = simulate_task_setup(
        task, sql_query=sql_query, task_config=task.config
    )

    assert setup_result.is_err


def test_autosql_task_config_error2():
    task = simulate_task("autosql")
    task.config = {
        "file_name": "test.sql",
        "materialisation": "wrong",
        "destination": {"table": "test_autosql_task"},
    }

    setup_result = simulate_task_setup(
        task, sql_query=sql_query, task_config=task.config
    )

    assert setup_result.is_err


def test_autosql_task_run_error():
    task = simulate_task("autosql")
    task.config = {
        "file_name": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query_err, task_config=task.config
    )

    assert setup_result.is_ok
    assert run_result.is_err


def test_autosql_task_run_ddl():
    task = simulate_task("autosql")
    task.config = {
        "file_name": "test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
        "ddl": {"indexes": {"primary_key": {"columns": ["x"]}}},
    }

    setup_result, run_result = simulate_task_setup_run(
        task, sql_query=sql_query, task_config=task.config
    )

    assert setup_result.is_ok
    assert run_result.is_err
    assert task.steps == [
        "Write Query",
        "Cleanup",
        "Create Temp",
        "Create Indexes",
        "Cleanup Target",
        "Move",
    ]


# def test_autosql_task_ddl_config_error():
#    task = simulate_task("autosql")
#    task.config = {
#        "file_name": "test.sql",
#        "materialisation": "table",
#        "destination": {"table": "test_autosql_task"},
#        "ddll": {"indexes": {"primary_key": {"columns": ["x"]}}}
#    }
#
#    setup_result = simulate_task_setup(task, sql_query=sql_query, task_config=task.config)
#
#    assert setup_result.is_err
