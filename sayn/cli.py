from pathlib import Path
from datetime import date, timedelta
import sys

import click

from .utils.python_loader import PythonLoader
from .utils.task_query import get_query
from .utils.graphviz import plot_dag
from .utils.logging import ConsoleLogger, ConsoleDebugLogger, FileLogger
from .scaffolding.init_project import sayn_init
from .core.app import App
from .core.config import read_project, read_dags, read_settings, get_tasks_dict
from .core.errors import Err, Ok, Result


class CliApp(App):
    def __init__(
        self,
        debug=False,
        include=list(),
        exclude=list(),
        profile=None,
        full_load=False,
        start_dt=date.today() - timedelta(days=1),
        end_dt=date.today() - timedelta(days=1),
    ):
        # STARTING APP: register loggers and set cli arguments in the App object
        if debug:
            self.tracker.register_logger(ConsoleDebugLogger())
        else:
            self.tracker.register_logger(ConsoleLogger())
        self.tracker.register_logger(FileLogger(self.run_arguments["folders"]["logs"]))

        self.set_run_arguments(
            debug=debug,
            full_load=full_load,
            start_dt=start_dt,
            end_dt=end_dt,
            profile=profile,
        )
        self.report_start_app()

        # SETUP THE APP: read project config and settings, interpret cli arguments and setup the dag
        self.report_start_setup()

        # Read the project configuration
        project = self.handle_result(read_project())
        dags = self.handle_result(read_dags(project.dags))
        self.set_project(project)
        settings = self.handle_result(read_settings())
        self.handle_result(self.set_settings(settings))

        # Set python environment
        self.python_loader = PythonLoader()
        if Path(self.run_arguments["folders"]["python"]).is_dir():
            self.handle_result(
                self.python_loader.register_module(
                    "python_tasks", self.run_arguments["folders"]["python"]
                )
            )

        # Set tasks and dag from it
        tasks_dict = self.handle_result(get_tasks_dict(project.presets, dags))
        task_query = self.handle_result(
            get_query(tasks_dict, include=include, exclude=exclude)
        )
        self.handle_result(self.set_tasks(tasks_dict, task_query))

        self.report_finish_setup(Ok())

    def handle_result(self, result):
        """Interpret the result of setup opreations returning the value if `result.is_ok`.

        Setup errors from the cli result in execution abort.

        Args:
          result (sayn.errors.Result): The result of a setup operation
        """
        if result is None or not isinstance(result, Result):
            self.report_finish_setup(Err("app_setup", "unhandled_error", result=result))
            sys.exit()
        elif result.is_err:
            import IPython

            IPython.embed()
            self.report_finish_setup(result)
            sys.exit()
        else:
            return result.value


# Click arguments

click_debug = click.option(
    "--debug", "-d", is_flag=True, default=False, help="Include debug messages"
)


def click_filter(func):
    func = click.option(
        "--tasks",
        "-t",
        multiple=True,
        help="Task query to INCLUDE in the execution: [+]task_name[+], dag:dag_name, tag:tag_name",
    )(func)
    func = click.option(
        "--exclude",
        "-x",
        multiple=True,
        help="Task query to EXCLUDE in the execution: [+]task_name[+], dag:dag_name, tag:tag_name",
    )(func)
    return func


def click_incremental(func):
    func = click.option(
        "--full-load", "-f", is_flag=True, default=False, help="Do a full load"
    )(func)
    func = click.option(
        "--start-dt",
        "-s",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today() - timedelta(days=1)),
        help="For incremental loads, the start date",
    )(func)
    func = click.option(
        "--end-dt",
        "-e",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today() - timedelta(days=1)),
        help="For incremental loads, the end date",
    )(func)
    return func


def click_run_options(func):
    func = click_debug(func)
    func = click.option("--profile", "-p", help="Profile from settings to use")(func)
    func = click_incremental(func)
    func = click_filter(func)
    return func


@click.group(help="SAYN management tool.")
def cli():
    pass


# Click commands


@cli.command(help="Initialise a SAYN project in working directory.")
@click.argument("sayn_project_name")
def init(sayn_project_name):
    sayn_init(sayn_project_name)


@cli.command(help="Compile sql tasks.")
@click_run_options
def compile(debug, tasks, exclude, profile, full_load, start_dt, end_dt):
    app = CliApp(debug, tasks, exclude, profile, full_load, start_dt, end_dt)
    app.compile()


@cli.command(help="Run SAYN tasks.")
@click_run_options
def run(debug, tasks, exclude, profile, full_load, start_dt, end_dt):
    app = CliApp(debug, tasks, exclude, profile, full_load, start_dt, end_dt)
    app.run()


@cli.command(help="Generate DAG image.")
@click_debug
@click_filter
def dag_image(debug, tasks, exclude):
    project = read_project()
    dags = read_dags(project.dags)
    tasks_dict = get_tasks_dict(project.presets, dags)
    dag = {
        task["name"]: [p for p in task.get("parents", list())]
        for task in tasks_dict.values()
    }
    plot_dag(dag, "images", "dag")
