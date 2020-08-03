from datetime import date, timedelta

# import sys

import click

# from ..config import Config, SaynConfigError
from ..start_project.start_project import sayn_init
from ..app.common import SaynApp

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
        # type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today() + timedelta(days=-1)),
        help="For incremental loads, the start date",
    )(func)
    func = click.option(
        "--end-dt",
        "-e",
        # type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today() + timedelta(days=-1)),
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
    app = SaynApp(debug=debug)
    app.setup(tasks, exclude, profile, full_load, start_dt, end_dt)
    app.compile()


@cli.command(help="Run SAYN tasks.")
@click_run_options
def run(debug, tasks, exclude, profile, full_load, start_dt, end_dt):
    app = SaynApp(debug=debug)
    app.setup(tasks, exclude, profile, full_load, start_dt, end_dt)
    app.run()


@cli.command(help="Generate DAG image")
@click_debug
@click_filter
def dag_image(debug, tasks, exclude):
    pass
    # run_start_ts = datetime.now()
    # run_id = (run_start_ts - datetime(1970, 1, 1)).total_seconds()

    # ui = UI(run_id=run_id, debug=debug)
    # ui._set_config(stage_name="setup")

    # try:
    #     Config()
    #     ui._status_success("Config set.")
    # except SaynConfigError as e:
    #     ui._status_fail(f"{e}")
    #     sys.exit(1)

    # dag = Dag(tasks_query=tasks, exclude_query=exclude)
    # dag.plot(folder="images", file_name="dag")
