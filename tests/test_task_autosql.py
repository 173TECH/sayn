from . import inside_dir, simulate_task

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_err = "SELECTS * FROM non_existing_table"


def test_autosql_task_table(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_ok
        assert task.sql_query == sql_query
        assert task.steps == [
            "Write Query",
            "Cleanup",
            "Create Temp",
            "Cleanup Target",
            "Move",
        ]

        # run
        run_result = task.run()
        task_result = task.default_db.select("SELECT * FROM test_autosql_task")
        task_table = task.default_db.select(
            'SELECT * FROM sqlite_master WHERE type="table" AND NAME = "test_autosql_task"'
        )
        assert run_result.is_ok
        assert task_result[0]["x"] == 1
        assert len(task_table) == 1


def test_autosql_task_view(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "view",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_ok
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query", "Cleanup Target", "Create View"]

        # run
        run_result = task.run()
        task_result = task.default_db.select("SELECT * FROM test_autosql_task")
        task_table = task.default_db.select(
            'SELECT * FROM sqlite_master WHERE type="view" AND NAME = "test_autosql_task"'
        )
        assert run_result.is_ok
        assert task_result[0]["x"] == 1
        assert len(task_table) == 1


def test_autosql_task_compile(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)
        task.run_arguments.update({"command": "compile"})

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
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

        # compile
        compile_result = task.compile()
        assert compile_result.is_ok


def test_autosql_task_param(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_param)
        task.task_parameters = {"number": 1}
        task.jinja_env.globals.update(**task.task_parameters)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_ok

        # run
        run_result = task.run()
        task_result = task.default_db.select("SELECT * FROM test_autosql_task")
        assert run_result.is_ok
        assert task_result[0]["x"] == 1


def test_autosql_task_config_error1(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            **{
                "file_nam": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_err


def test_autosql_task_config_error2(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "wrong",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_err


def test_autosql_task_config_error3(tmp_path):
    # tests missing parameters for jinja compilation
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_param)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_err


def test_autosql_task_run_error(tmp_path):
    # tests failure with erratic sql
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
            }
        )
        assert setup_result.is_ok

        # run
        run_result = task.run()
        assert run_result.is_err


def test_autosql_task_run_ddl(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            **{
                "file_name": "test.sql",
                "materialisation": "table",
                "destination": {"table": "test_autosql_task"},
                "ddl": {"indexes": {"primary_key": {"columns": ["x"]}}},
            }
        )
        assert setup_result.is_ok
        assert task.steps == [
            "Write Query",
            "Cleanup",
            "Create Temp",
            "Create Indexes",
            "Cleanup Target",
            "Move",
        ]

        # run
        # this needs to be reimplemented when PRIMARY KEY setup fixed for SQLite
        # run_result = task.run()
        # assert run_result.is_ok
