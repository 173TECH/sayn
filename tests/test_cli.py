import pytest

import sayn.cli as tcli

import click
from click.testing import CliRunner

@click.group()
def cli():
    pass


@cli.command()
@tcli.click_run_options
def run(debug, tasks, exclude, profile, full_load, start_dt, end_dt):

    tasks = [eval(t) for t in tasks]
    exclude = [eval(e) for e in exclude]

    tasks = [i for t in tasks for i in t]
    exclude = [i for t in exclude for i in t]

    return {
            "debug":debug,
            "tasks":tasks,
            "exclude":exclude,
            "profile":profile,
            "full_load":full_load,
            "start_dt":start_dt,
            "end_dt":end_dt
           }


@cli.command()
@tcli.click_run_options
def compile(debug, tasks, exclude, profile, full_load, start_dt, end_dt):

    tasks = [eval(t) for t in tasks]
    exclude = [eval(e) for e in exclude]

    tasks = [i for t in tasks for i in t]
    exclude = [i for t in exclude for i in t]

    return {
            "debug":debug,
            "tasks":tasks,
            "exclude":exclude,
            "profile":profile,
            "full_load":full_load,
            "start_dt":start_dt,
            "end_dt":end_dt
           }


def test_run01():
    runner = CliRunner()
    result1 = runner.invoke(run, ['-t', 'test1', 'test2'], standalone_mode=False)
    result2 = runner.invoke(run, ['-t', 'test1', '-t', 'test2'], standalone_mode=False)
    assert result1.return_value == result2.return_value
    assert result1.exit_code == 0
    assert result2.exit_code == 0


def test_compile01():
    runner = CliRunner()
    result1 = runner.invoke(compile, ['-t', 'test1', 'test2'], standalone_mode=False)
    result2 = runner.invoke(compile, ['-t', 'test1', '-t', 'test2'], standalone_mode=False)
    assert result1.return_value == result2.return_value
    assert result1.exit_code == 0
    assert result2.exit_code == 0
