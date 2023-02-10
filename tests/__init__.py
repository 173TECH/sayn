from contextlib import contextmanager
import os
from pathlib import Path
import subprocess


from sayn.core.app import RunArguments
from sayn.database.creator import create as create_db
from sayn.utils.compiler import Compiler


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

    def info(self, msg):
        pass


vd = VoidTracker()


def simulate_task(
    task_class,
    used_objects,
    source_db=None,
    target_db=None,
    run_arguments=None,
    task_params=None,
):

    run_arguments = run_arguments or dict()
    task_params = task_params or dict()

    connections = dict()
    if target_db is not None:
        connections["target_db"] = create_db(
            "target_db",
            "target_db",
            target_db.copy(),
        )

    if source_db is not None:
        connections["source_db"] = create_db(
            "source_db",
            "source_db",
            source_db.copy(),
        )

    for c in connections.values():
        c._activate_connection()

    obj_run_arguments = RunArguments()
    obj_run_arguments.update(**run_arguments)

    run_arguments = {
        "debug": obj_run_arguments.debug,
        "full_load": obj_run_arguments.full_load,
        "start_dt": obj_run_arguments.start_dt,
        "end_dt": obj_run_arguments.end_dt,
        "command": obj_run_arguments.command.value,
        "folders": {
            "python": obj_run_arguments.folders.python,
            "sql": obj_run_arguments.folders.sql,
            "compile": obj_run_arguments.folders.compile,
            "logs": obj_run_arguments.folders.logs,
            "tests": obj_run_arguments.folders.tests,
        },
    }

    base_compiler = Compiler(obj_run_arguments, dict(), task_params)

    class DBObjectUtil:
        reference_level = {"db": 2, "schema": 1, None: 0}

        def __init__(self, used_objects):
            self.to_introspect = used_objects

        def src_out(self, obj, connection=None, level=None):
            if level is not None:
                level_value = self.reference_level[level]
                obj += "." * level_value
            else:
                level_value = obj.count(".")

            if level_value == 0:
                database = ""
                schema = ""
                table = obj
            elif level_value == 1:
                database = ""
                schema = obj.split(".")[0]
                table = obj.split(".")[1]
            elif level_value == 2:
                database = obj.split(".")[0]
                schema = obj.split(".")[1]
                table = obj.split(".")[2]

            if isinstance(connection, str):
                connection_name = connection
            else:
                connection_name = connection.name

            if connection_name not in self.to_introspect:
                self.to_introspect[connection_name] = {database: dict()}

            if database not in self.to_introspect[connection_name]:
                self.to_introspect[connection_name][database] = dict()

            if schema not in self.to_introspect[connection_name][database]:
                self.to_introspect[connection_name][database][schema] = set()

            self.to_introspect[connection_name][database][schema].add(table)

            return obj

    task_compiler = base_compiler.get_task_compiler("test_group", "test_task")
    task_compiler.update_globals(**task_params)

    obj_util = DBObjectUtil(used_objects)

    task = task_class(
        "test_task",
        "test_group",
        vd,
        run_arguments,
        task_params,
        dict(),
        "target_db",
        connections,
        task_compiler,
        obj_util.src_out,
        obj_util.src_out,
    )

    return task


def validate_table(db, table_name, expected_data, variable_columns=None):
    result = db.read_data(f"select * from {table_name}")
    if len(result) != len(expected_data):
        return False

    result = sorted(result, key=lambda x: list(x.values()))
    expected_data = sorted(expected_data, key=lambda x: list(x.values()))
    for i in range(len(result)):
        if variable_columns is None:
            if result[i] != expected_data[i]:
                return False
        else:
            expected_clean = {
                k: v for k, v in expected_data[i].items() if k not in variable_columns
            }
            result_clean = {
                k: v for k, v in result[i].items() if k not in variable_columns
            }

            if expected_clean != result_clean:
                return False

            if result[i].keys() != expected_data[i].keys():
                return False

    return True


@contextmanager
def tables_with_data(db, tables, extra_tables=list()):
    tables_to_delete = extra_tables.copy()
    for table, data in tables.items():
        if isinstance(table, tuple):
            schema = table[0]
            table = table[1]
            tables_to_delete.append(f"{schema}.{table}")
        else:
            schema = None
            tables_to_delete.append(table)

        db.load_data(table, data, schema=schema, replace=True)

    try:
        yield
    finally:
        clear_tables(db, tables_to_delete)


def clear_tables(db, tables):
    for table in tables:
        try:
            db.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            pass

        try:
            db.execute(f"DROP VIEW IF EXISTS {table}")
        except:
            pass
