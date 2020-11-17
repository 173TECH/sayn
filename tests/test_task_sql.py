from . import inside_dir, simulate_task

sql_query = "CREATE TABLE test_sql_task AS SELECT 1 AS x"
sql_query_param = "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x"
sql_query_err = "SELECT * FROM non_existing_table"


def test_sql_task(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query)

        # setup
        setup_result = task.setup("test.sql")
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query", "Execute Query"]
        assert setup_result.is_ok

        # run
        run_result = task.run()
        task_result = task.default_db.select("SELECT * FROM test_sql_task")
        assert task_result[0]["x"] == 1
        assert run_result.is_ok


def test_sql_task_compile(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query)
        task.run_arguments.update({"command": "compile"})

        # setup
        setup_result = task.setup("test.sql")
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query"]
        assert setup_result.is_ok

        # compile
        compile_result = task.compile()
        assert compile_result.is_ok


def test_sql_task_param(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_param)
        task.task_parameters = {"user_prefix": "tu_"}
        task.jinja_env.globals.update(**task.task_parameters)

        # setup
        setup_result = task.setup("test.sql")
        assert task.sql_query == "CREATE TABLE tu_test_sql_task AS SELECT 1 AS x"
        assert setup_result.is_ok

        # run
        run_result = task.run()
        task_result = task.default_db.select("SELECT * FROM tu_test_sql_task")
        assert task_result[0]["x"] == 1
        assert run_result.is_ok


def test_sql_task_param_err(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_param)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_err


def test_sql_task_run_err(tmp_path):
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok

        # run
        run_result = task.run()
        assert run_result.is_err
