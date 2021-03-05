from contextlib import contextmanager
from itertools import product
import os
from pathlib import Path
import subprocess

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from ruamel.yaml import YAML

from sayn.database.creator import create as create_db


def get_dbs():
    """Get a list of database configurations from environment variables"""
    dbs = [
        (k[len("TEST_DB_") :], YAML().load(v))
        for k, v in os.environ.items()
        if k.startswith("TEST_DB_")
    ]

    if len(dbs) == 0:
        dbs = [("sqlite", {"type": "sqlite", "database": ":memory:"})]

    return dbs


def pytest_generate_tests(metafunc):
    """Dynamically generates tests based on parameters.

    Currently, for tests that use databases, this generates 2 fixtures: source_db and target_db.
    Including these parameters in a test trigger this dynamic generation.
    Defaults to sqlite in memory databases.
    """
    dbs = get_dbs()
    if "target_db" in metafunc.fixturenames and "source_db" in metafunc.fixturenames:
        db_pairs = list(product(dbs, dbs))
        metafunc.parametrize(
            "source_db,target_db",
            [(c[0][1], c[1][1]) for c in db_pairs],
            ids=[f"src:{c[0][0]},dst:{c[1][0]}" for c in db_pairs],
        )
    elif "target_db" in metafunc.fixturenames:
        metafunc.parametrize("target_db", [d[1] for d in dbs], ids=[d[0] for d in dbs])


@contextmanager
def inside_dir(dirpath, fs=dict()):
    """
    Execute code from inside the given directory
    :param dirpath: String, path of the directory the command is being run.
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        for filepath, content in fs.items():
            fpath = Path(filepath)
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content)
        yield
    finally:
        os.chdir(old_path)


@contextmanager
def create_project(dirpath, settings=None, project=None, groups=dict(), env=dict()):
    """
    Execute code from inside the given directory, creating the sayn project files
    :param settings: String, yaml for a settings.yaml file
    :param project: String, yaml for a project.yaml file
    :param groups: Dict, dict of yaml for the contents of the tasks folder
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        if settings is not None:
            Path(dirpath, "settings.yaml").write_text(settings)
        if project is not None:
            Path(dirpath, "project.yaml").write_text(project)
        if len(groups) > 0:
            for name, group in groups.items():
                Path(dirpath, f"{name}.yaml").write_text(group)
        if len(env) > 0:
            os.environ.update(env)
        yield
    finally:
        os.chdir(old_path)
        for k in env.keys():
            del os.environ[k]


def run_sayn(*args):
    return subprocess.check_output(
        f"sayn {' '.join(args)}", shell=True, stderr=subprocess.STDOUT
    )


# Task Simulators

# create empty tracker class to enable the run to go through
class VoidTracker:
    def set_run_steps(self, steps):
        pass

    def start_step(self, step):
        pass

    def finish_current_step(self):
        pass


vd = VoidTracker()


def simulate_task(
    task,
    source_db=None,
    target_db=None,
    run_arguments=dict(),
    task_params=dict(),
):
    task.name = "test_task"  # set for compilation output during run
    task.group = "test_group"  # set for compilation output during run
    task.run_arguments = {
        "folders": {"sql": "sql", "compile": "compile"},
        "command": "run",
        "debug": False,
        "full_load": False,
        **run_arguments,
    }

    if target_db is not None:
        task.connections = {
            "target_db": create_db("target_db", "target_db", target_db.copy())
        }

    if source_db is not None:
        task.connections.update(
            {"source_db": create_db("source_db", "source_db", source_db.copy())}
        )

    task._default_db = "target_db"
    task.tracker = vd

    task.jinja_env = Environment(
        loader=FileSystemLoader(os.getcwd()),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    task.jinja_env.globals.update(**task_params)


def validate_table(db, table_name, expected_data):
    result = db.read_data(f"select * from {table_name}")
    if len(result) != len(expected_data):
        return False
    for i in range(len(result)):
        if result[i] != expected_data[i]:
            return False
    return True


@contextmanager
def tables_with_data(db, tables, extra_tables=list()):
    for table, data in tables.items():
        db.load_data(table, data, replace=True)

    try:
        yield
    finally:
        clear_tables(db, list(tables.keys()) + extra_tables)


def clear_tables(db, tables):
    for table in tables:
        try:
            db.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            try:
                db.execute(f"DROP VIEW IF EXISTS {table}")
            except:
                pass
