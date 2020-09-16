from contextlib import contextmanager
import os
from pathlib import Path
import subprocess


@contextmanager
def inside_dir(dirpath):
    """
    Execute code from inside the given directory
    :param dirpath: String, path of the directory the command is being run.
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        yield
    finally:
        os.chdir(old_path)


@contextmanager
def create_project(dirpath, settings=None, project=None, dags=dict(), env=dict()):
    """
    Execute code from inside the given directory, creating the sayn project files
    :param settings: String, yaml for a settings.yaml file
    :param project: String, yaml for a project.yaml file
    :param dags: Dict, dict of yaml for the contents of the dags folder
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        if settings is not None:
            Path(dirpath, "settings.yaml").write_text(settings)
        if project is not None:
            Path(dirpath, "project.yaml").write_text(project)
        if len(dags) > 0:
            for name, dag in dags.items():
                Path(dirpath, f"{name}.yaml").write_text(dag)
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
