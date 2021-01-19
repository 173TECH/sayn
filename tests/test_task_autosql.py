from . import inside_dir, simulate_task, validate_table

sql_query = "SELECT 1 AS x"
sql_query_param = "SELECT {{number}} AS x"
sql_query_ddl_diff_col_order = "SELECT CAST(1 AS INTEGER) AS y, CAST(1 AS TEXT) AS x"


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
        task.target_db._introspect()

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_autosql_task", [{"x": 1}])
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
        task.target_db._introspect()

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
        sql_query_incremental = (
            "SELECT * FROM source_table WHERE updated_at >= 2 OR updated_at IS NULL"
        )
        task = simulate_task("autosql", sql_query=sql_query_incremental)

        # create source table
        task.default_db.execute(
            "CREATE TABLE source_table (id int, updated_at int, name text);"
            'INSERT INTO source_table SELECT 1, 1, "x" UNION SELECT 2, 2, "y1" UNION SELECT 3, NULL, "z"'
        )

        # create model table
        task.default_db.execute(
            "CREATE TABLE test_autosql_task (id int, updated_at int, name text);"
            'INSERT INTO test_autosql_task SELECT 1, 1, "x" UNION SELECT 2, NULL, "y"'
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
        task.target_db._introspect()

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
        assert setup_result.is_ok
        task.target_db._introspect()

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
        task.target_db._introspect()

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
        sql_query_err = "SELECT * FROM non_existing_table"
        task = simulate_task("autosql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
        )
        assert setup_result.is_ok
        task.target_db._introspect()

        # run
        run_result = task.run()
        assert run_result.is_err


# Destination tests


def test_autosql_task_table_db_dst(tmp_path):
    # test autosql with db destination set
    with inside_dir(tmp_path):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"db": "target_db", "table": "test_autosql_task"},
        )
        assert setup_result.is_ok
        task.target_db._introspect()

        # run
        run_result = task.run()
        assert run_result.is_ok
        target_db = task.connections["target_db"]
        assert validate_table(target_db, "test_autosql_task", [{"x": 1}],)
        assert (
            len(
                target_db.read_data(
                    'SELECT * FROM sqlite_master WHERE type="table" AND NAME = "test_autosql_task"'
                )
            )
            == 1
        )


def test_autosql_task_table_wrong_db_dst(tmp_path):
    # test autosql with db destination set but does not exist in connections
    with inside_dir(tmp_path):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"db": "wrong_dst", "table": "test_autosql_task"},
        )
        assert setup_result.is_err


# DDL tests


def test_autosql_task_run_ddl_columns(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={"columns": [{"name": "x", "type": "integer", "primary": True}]},
        )
        assert setup_result.is_ok
        task.target_db._introspect()

        # run
        run_result = task.run()
        assert run_result.is_ok
        # test the pk has indeed been set
        pk_info = task.default_db.read_data("PRAGMA table_info(test_autosql_task)")
        assert pk_info[0]["pk"] == 1


def test_autosql_task_run_indexes_pk01(tmp_path):
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
        assert setup_result.is_err


def test_autosql_task_run_indexes_pk02(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("autosql", sql_query=sql_query)

        # setup
        setup_result = task.setup(
            file_name="test.sql",
            materialisation="table",
            destination={"table": "test_autosql_task"},
            ddl={"columns": ["x"], "indexes": [{"primary_key": "x"}]},
        )
        assert setup_result.is_err


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
        task.target_db._introspect()

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(
            task.default_db, "test_autosql_task", [{"x": "1", "y": 1}],
        )
