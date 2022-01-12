from datetime import date, timedelta
import sys

import click

from .utils.graphviz import plot_dag
from .logging import ConsoleLogger, FancyLogger, FileLogger
from .scaffolding.init_project import sayn_init
from .core.app import App, Command
from .core.project import read_project, read_groups, get_tasks_dict
from .tasks.task import TaskStatus

yesterday = date.today() - timedelta(days=1)


class CliApp(App):
    def __init__(
        self,
        command,
        debug=False,
        include=None,
        exclude=None,
        profile=None,
        full_load=False,
        start_dt=yesterday,
        end_dt=yesterday,
    ):
        super().__init__()

        # STARTING APP: register loggers and set cli arguments in the App object
        self.run_arguments.command = command

        if debug:
            self.run_arguments.debug = debug
        else:
            self.tracker.remove_logger(ConsoleLogger)
            self.tracker.register_logger(FancyLogger())

        self.tracker.register_logger(
            FileLogger(
                self.run_arguments.folders.logs,
                format=f"{self.run_id}|" + "%(asctime)s|%(levelname)s|%(message)s",
            )
        )

        self.run_arguments.start_dt = start_dt.date()
        self.run_arguments.end_dt = end_dt.date()

        self.run_arguments.profile = profile
        self.run_arguments.full_load = full_load

        if include is not None:
            self.run_arguments.include = include

        if exclude is not None:
            self.run_arguments.exclude = exclude

        self.start_app()


class ChainOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.save_other_options = kwargs.pop("save_other_options", True)
        nargs = kwargs.pop("nargs", -1)
        assert nargs == -1, "nargs, if set, must be -1 not {}".format(nargs)
        super(ChainOption, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):
        def parser_process(value, state):
            # method to hook to the parser.process
            done = False
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value += f" {state.rargs.pop(0)}"
            else:
                # grab everything remaining
                value += state.rargs
                state.rargs[:] = []

            # call the actual process
            self._previous_parser_process(value, state)

        retval = super(ChainOption, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval


# Click arguments

click_debug = click.option(
    "--debug", "-d", is_flag=True, default=False, help="Include debug messages"
)


def click_filter(func):
    func = click.option(
        "--tasks",
        "-t",
        multiple=True,
        cls=ChainOption,
        help="Task query to INCLUDE in the execution: [+]task_name[+], group:group_name, tag:tag_name",
        default=list(),
    )(func)
    func = click.option(
        "--exclude",
        "-x",
        multiple=True,
        cls=ChainOption,
        help="Task query to EXCLUDE in the execution: [+]task_name[+], group:group_name, tag:tag_name",
        default=list(),
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
        default=str(yesterday),
        help="For incremental loads, the start date",
    )(func)
    func = click.option(
        "--end-dt",
        "-e",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(yesterday),
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

    tasks = [i for t in tasks for i in t.strip().split(" ")]
    exclude = [i for t in exclude for i in t.strip().split(" ")]
    app = CliApp(
        Command.COMPILE, debug, tasks, exclude, profile, full_load, start_dt, end_dt
    )

    app.compile()
    if any([t.status == TaskStatus.FAILED for _, t in app.tasks.items()]):
        sys.exit(-1)
    else:
        sys.exit()


@cli.command(help="Run SAYN tasks.")
@click_run_options
def run(debug, tasks, exclude, profile, full_load, start_dt, end_dt):

    tasks = [i for t in tasks for i in t.strip().split(" ")]
    exclude = [i for t in exclude for i in t.strip().split(" ")]
    app = CliApp(
        Command.RUN, debug, tasks, exclude, profile, full_load, start_dt, end_dt
    )

    app.run()
    if any([t.status == TaskStatus.FAILED for _, t in app.tasks.items()]):
        sys.exit(-1)
    else:
        sys.exit()


@cli.command(help="Test SAYN tasks.")
@click_run_options
def test(debug, tasks, exclude, profile, full_load, start_dt, end_dt):

    tasks = [i for t in tasks for i in t.strip().split(" ")]
    exclude = [i for t in exclude for i in t.strip().split(" ")]
    app = CliApp(
        Command.TEST, debug, tasks, exclude, profile, full_load, start_dt, end_dt
    )

    app.test()
    if any([t.status == TaskStatus.FAILED for _, t in app.tasks.items()]):
        sys.exit(-1)
    else:
        sys.exit()


@cli.command(help="Generate DAG image.")
@click_debug
@click_filter
def dag_image(debug, tasks, exclude):
    def handle_error():
        print("Errors detected in project. Run `sayn compile` to see the errors")
        sys.exit()

    result = read_project()
    if result.is_err:
        handle_error()
    else:
        project = result.value

    result = read_groups(project.groups)
    if result.is_err:
        handle_error()
    else:
        groups = result.value

    result = get_tasks_dict(project.presets, groups)
    if result.is_err:
        handle_error()
    else:
        tasks_dict = result.value
    dag = {
        task["name"]: [p for p in task.get("parents", list())]
        for task in tasks_dict.values()
    }

    plot_dag(dag, "images", "dag")

    print("Dag image created in `images/dag.png`")
