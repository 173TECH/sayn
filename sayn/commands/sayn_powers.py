from datetime import datetime, date, timedelta
import logging
import sys

import click

from ..config import Config, SaynConfigError
from ..utils.logger import Logger
from ..dag import Dag
from ..start_project.start_project import sayn_init

run_start_ts = datetime.now()
run_id = (run_start_ts - datetime(1970, 1, 1)).total_seconds()
Logger(run_id=run_id)

# Command options shared among several commands
click_debug = click.option(
    "--debug", "-d", is_flag=True, default=False, help="Run in debug mode",
)

click_tasks = click.option(
    "--task", "-t", multiple=False, help="Filter on this task. Defaults to all tasks",
)

click_models = click.option(
    "--model", "-m", multiple=False, help="Filter on this model. Defaults to all tasks",
)

click_profile = click.option("--profile", "-p", help="Profile from settings to use")

click_full_load = click.option(
    "--full-load", "-f", is_flag=True, default=False, help="Do a full load"
)

click_start_dt = click.option(
    "--start-dt",
    "-s",
    # type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() + timedelta(days=-1)),
    help="For incremental loads, the start date",
)

click_end_dt = click.option(
    "--end-dt",
    "-e",
    # type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() + timedelta(days=-1)),
    help="For incremental loads, the end date",
)


def click_base(func):
    func = click.option(
        "--debug", "-d", is_flag=True, default=False, help="Include debug messages"
    )(func)
    return func


def click_filter(func):
    func = click_tasks(func)
    func = click_models(func)
    return func


def click_run_options(func):
    func = click_profile(func)
    func = click_full_load(func)
    func = click_start_dt(func)
    func = click_end_dt(func)
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
@click_base
@click_filter
@click_run_options
def compile(debug, task, model, profile, full_load, start_dt, end_dt):
    run_command("compile", debug, task, model, profile, full_load, start_dt, end_dt)


@cli.command(help="Run SAYN tasks.")
@click_base
@click_filter
@click_run_options
def run(debug, task, model, profile, full_load, start_dt, end_dt):
    run_command("run", debug, task, model, profile, full_load, start_dt, end_dt)


@cli.command(help="Generate DAG image")
@click_base
@click_filter
def dag_image(debug, task, model):
    try:
        Config()
    except SaynConfigError as e:
        logging.error(e)
        sys.exit(1)

    Dag(task=task).plot_dag(folder="images", file_name="dag")


def run_command(command, debug, task, model, profile, full_load, start_dt, end_dt):
    if debug:
        Logger().set_debug()
    try:
        Config(
            profile=profile,
            full_load=full_load,
            start_dt=start_dt,
            end_dt=end_dt,
            debug=debug,
        )
    except SaynConfigError as e:
        logging.error(e)
        sys.exit(1)

    dag = Dag(task=task, model=model)

    if command == "run":
        dag.run()
    elif command == "compile":
        dag.compile()
    else:
        raise ValueError(f'Unknown command "{command}"')

    logging.info(f"{command.capitalize()} took {datetime.now() - run_start_ts}")
