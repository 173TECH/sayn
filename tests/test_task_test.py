from contextlib import contextmanager

import pytest
from sayn.tasks.test import TestTask as Task

from . import inside_dir, simulate_task, tables_with_data, validate_table, clear_tables


@contextmanager
def task_tset(tmp_path, target_db, sql, data=None, **kwargs):
    """Creates an test task and drops the tables/views created after it's done"""

    fs = {"tests/test.sql": sql} if sql is not None else dict()
    with inside_dir(tmp_path, fs):
        task = simulate_task(Task, target_db=target_db, **kwargs)
        task.run_arguments["command"] = "test"
        if data is not None:
            with tables_with_data(task.connections["target_db"], data):
                yield task
        else:
            yield task


def test_custom_test(tmp_path, target_db):
    with task_tset(tmp_path, target_db, "SELECT 1 AS x WHERE x IS NULL") as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.test().is_ok


def test_custom_test_fail(tmp_path, target_db):
    with task_tset(tmp_path, target_db, "SELECT 1 AS x WHERE x IS NOT NULL") as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.test().is_err
