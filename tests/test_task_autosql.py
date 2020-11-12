import tempfile
import sqlite3
from pathlib import Path
import pytest

from . import inside_dir, simulate_sql_task
from sayn.core.errors import Result

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_err = "SELECTS * FROM non_existing_table"


def test_autosql_task_table():
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = task.setup(**task.config)
        run_result = task.run()

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
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "view",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = task.setup(**task.config)
        run_result = task.run()

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
    task = simulate_sql_task("autosql")
    task.run_arguments.update(
        {"command": "compile", "debug": False, "full_load": False}
    )
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = task.setup(**task.config)
        run_result = task.run()

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
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.task_parameters = {"number": 1}
    task.jinja_env.globals.update(**task.task_parameters)
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query_param)
        setup_result = task.setup(**task.config)
        run_result = task.run()

    task_result = task.default_db.select("SELECT * FROM test_autosql_task")

    assert task_result[0]["x"] == 1
    assert setup_result.is_ok
    assert run_result.is_ok


def test_autosql_task_config_error1():
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.config = {
        "file_nam": "autosql_task_test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = task.setup(**task.config)

    assert setup_result.is_err


def test_autosql_task_config_error2():
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "wrong",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query)
        setup_result = task.setup(**task.config)

    assert setup_result.is_err


def test_autosql_task_run_error():
    task = simulate_sql_task("autosql")
    task.run_arguments.update({"debug": False, "full_load": False})
    task.config = {
        "file_name": "autosql_task_test.sql",
        "materialisation": "table",
        "destination": {"table": "test_autosql_task"},
    }

    tmp_path = tempfile.mkdtemp("tmp_test")
    with inside_dir(str(tmp_path)):
        fpath = Path(str(tmp_path), "sql", "autosql_task_test.sql")
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(sql_query_err)
        setup_result = task.setup(**task.config)
        run_result = task.run()

    assert setup_result.is_ok
    assert run_result.is_err
