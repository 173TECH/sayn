from . import inside_dir, simulate_task, validate_table

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_err = "SELECTS * FROM non_existing_table"
sql_query_ddl_diff_col_order = "SELECT CAST(1 AS INTEGER) AS y, CAST(1 AS TEXT) AS x"
sql_query_incremental = (
    "SELECT * FROM source_table WHERE updated_at >= 2 OR updated_at IS NULL"
)


def test_autosql_task_table(tmp_path):
    with inside_dir(tmp_path):
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
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_autosql_task", [{"x": 1}],)
        assert (
            len(
                task.default_db.read_data(
                    'SELECT * FROM sqlite_master WHERE type="table" AND NAME = "test_autosql_task"'
                )
            )
            == 1
        )


def test_autosql_task_view(tmp_path):
    with inside_dir(tmp_path):
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
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_autosql_task", [{"x": 1}],)
        assert (
            len(
                task.default_db.read_data(
                    'SELECT * FROM sqlite_master WHERE type="view" AND NAME = "test_autosql_task"'
                )
            )
            == 1
        )


def test_autosql_task_incremental(tmp_path):
    with inside_dir(tmp_path):
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
        assert validate_table(
            task.default_db,
            "test_autosql_task",
            [
                {"id": 1, "updated_at": 1, "name": "x"},
                {"id": 2, "updated_at": 2, "name": "y1"},
                {"id": 3, "updated_at": None, "name": "z"},
            ],
        )


def test_autosql_task_compile(tmp_path):
    with inside_dir(tmp_path):
        task = simulate_task(
            "autosql", sql_query=sql_query, run_arguments={"command": "compile"}
        )

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
    with inside_dir(tmp_path):
        task = simulate_task(
            "autosql", sql_query=sql_query_param, task_params={"number": 1}
        )

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_ok

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_autosql_task", [{"x": 1}],)


def test_autosql_task_config_error1(tmp_path):
    with inside_dir(tmp_path):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_nam="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_err


def test_autosql_task_config_error2(tmp_path):
    with inside_dir(tmp_path):
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
    with inside_dir(tmp_path):
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
    with inside_dir(tmp_path):
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


def test_autosql_task_run_ddl_columns(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={"columns": [{"name": "x", "primary": True}]},
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
        # test the pk has indeed been set
        pk_info = task.default_db.read_data("PRAGMA table_info(test_autosql_task)")
        assert pk_info[0]["pk"] == 1


def test_autosql_task_run_indexes_pk(tmp_path):
    # test indexes with the primary key only returns error on SQLite
    # this is because SQLite requires primary keys to be defined in create table statement so columns definition is needed
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={"indexes": [{"primary_key": "x"}]},
        )
        if "NO ALTER INDEXES" not in task.default_db.sql_features:
            assert setup_result.is_ok
            assert task.steps == [
                "Write Query",
                "Cleanup",
                "Create Temp",
                "Cleanup Target",
                "Move",
            ]
        else:
            assert setup_result.is_err

        # run
        if "NO ALTER INDEXES" not in task.default_db.sql_features:
            run_result = task.run()
            assert run_result.is_ok


def test_autosql_task_ddl_diff_pk_err(tmp_path):
    # test autosql task set with different pks in indexes and columns setup error
    with inside_dir(tmp_path):
        task = simulate_task("autosql", sql_query=sql_query_ddl_diff_col_order)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={
                "columns": [
                    {"name": "x", "type": "text", "primary": True},
                    {"name": "y", "type": "int"},
                ],
                "indexes": {"primary_key": {"columns": ["y"]}},
            },
        )
        assert setup_result.is_err


def test_autosql_task_run_ddl_diff_col_order(tmp_path):
    # test that autosql with ddl columns creates a table with order similar to ddl definition
    with inside_dir(tmp_path):
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
        assert validate_table(
            task.default_db, "test_autosql_task", [{"x": "1", "y": 1}],
        )
