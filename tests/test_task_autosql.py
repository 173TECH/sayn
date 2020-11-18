from . import inside_dir, simulate_task

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_err = "SELECTS * FROM non_existing_table"
sql_query_ddl_diff_col_order = "SELECT CAST(1 AS INTEGER) AS y, CAST(1 AS TEXT) AS x"
sql_query_incremental = (
    "SELECT * FROM source_table WHERE updated_at >= 2 OR updated_at IS NULL"
)


def test_autosql_task_table(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
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
            file_name="test.sql",
            materialisation="view",
            destination={"table": "test_autosql_task"},
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


def test_autosql_task_incremental(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_incremental)

        # create source table
        task.default_db.execute(
            'CREATE TABLE source_table AS SELECT CAST(1 AS INT) AS id, CAST(1 AS INT) AS updated_at, CAST("x" AS TEXT) AS name UNION SELECT 2 AS id, 2 AS updated_at, "y1" AS name UNION SELECT 3 AS id, NULL AS updated_at, "z" AS name'
        )

        # create model table
        task.default_db.execute(
            'CREATE TABLE test_autosql_task AS SELECT CAST(1 AS INT) AS id, CAST(1 AS INT) AS updated_at, CAST("x" AS TEXT) AS name UNION SELECT 2 AS id, NULL AS updated_at, "y" AS name'
        )

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="incremental",
            destination={"table": "test_autosql_task"},
            delete_key="id",
        )
        assert setup_result.is_ok
        assert task.sql_query == sql_query_incremental
        assert task.steps == [
            "Write Query",
            "Cleanup",
            "Create Temp",
            "Merge",
        ]

        # run
        run_result = task.run()
        assert run_result.is_ok
        task_result = task.default_db.select("SELECT * FROM test_autosql_task")
        assert len(task_result) == 3
        assert task_result[0]["id"] == 1
        assert task_result[0]["name"] == "x"
        assert task_result[1]["id"] == 2
        assert task_result[1]["name"] == "y1"


def test_autosql_task_compile(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)
        task.run_arguments.update({"command": "compile"})

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
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
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
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
            file_nam="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_err


def test_autosql_task_config_error2(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="wrong",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_err


def test_autosql_task_config_error3(tmp_path):
    # tests missing parameters for jinja compilation
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_param)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_err


def test_autosql_task_run_error(tmp_path):
    # tests failure with erratic sql
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
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
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={"indexes": {"primary_key": {"columns": ["x"]}}},
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


def test_autosql_task_run_ddl_diff_col_order(tmp_path):
    # test that autosql with ddl columns creates a table with order similar to ddl definition
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query_ddl_diff_col_order)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={
                "columns": [
                    {"name": "x", "type": "text"},
                    {"name": "y", "type": "int"},
                ]
            },
        )
        assert setup_result.is_ok
        assert task.steps == [
            "Write Query",
            "Cleanup",
            "Create Temp",
            "Cleanup Target",
            "Move",
        ]

        # run
        run_result = task.run()
        assert run_result.is_ok
        task_result = task.default_db.select("SELECT * FROM test_autosql_task")
        assert len(task_result) == 1
        table_result = task.default_db.get_table("test_autosql_task", None)
        assert table_result.columns.keys() == ["x", "y"]
