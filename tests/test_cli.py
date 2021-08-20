import pytest
from datetime import date, timedelta, datetime
import sayn.cli as tcli

import click
from click.testing import CliRunner


yesterday = date.today() - timedelta(days=1)

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

    start_dt = start_dt.date()
    end_dt = end_dt.date()

    return {
            "command":'run',
            "debug":debug,
            "include":tasks,
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

    start_dt = start_dt.date()
    end_dt = end_dt.date()

    return {
            "command":'compile',
            "debug":debug,
            "include":tasks,
            "exclude":exclude,
            "profile":profile,
            "full_load":full_load,
            "start_dt":start_dt,
            "end_dt":end_dt
           }

def get_output(command):
    runner = CliRunner()
    result = runner.invoke(cli, command, standalone_mode=False)
    output = result.return_value
    # return output
    return output


def test_simple_run():
    output = get_output("run")

    assert output == {
        "command": "run",
        "debug": False,
        "include": [],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_simple_compile():
    output = get_output("compile")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": [],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_one_task():
    output = get_output("run -t something".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }

def test_run_two_tasks_old():
    output = get_output("run -t something -t somethingelse".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": ['something', 'somethingelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_two_tasks_new():
    output = get_output("run -t something somethingelse".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": ['something', 'somethingelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_one_task():
    output = get_output("compile -t something".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }

def test_compile_two_tasks_old():
    output = get_output("compile -t something -t somethingelse".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ['something', 'somethingelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_two_tasks_new():
    output = get_output("compile -t something somethingelse".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ['something', 'somethingelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_debug():
    output = get_output("run -t something -d".split(' '))

    assert output == {
        "command": "run",
        "debug": True,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_debug():
    output = get_output("compile -t something -d".split(' '))

    assert output == {
        "command": "compile",
        "debug": True,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_debug_multitasks():
    output = get_output("run -t something -d -t somethingelse somesomeelse".split(' '))

    assert output == {
        "command": "run",
        "debug": True,
        "include": ['something', 'somethingelse', 'somesomeelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_debug_multitasks():
    output = get_output("compile -t something -d -t somethingelse somesomeelse".split(' '))

    assert output == {
        "command": "compile",
        "debug": True,
        "include": ['something', 'somethingelse', 'somesomeelse'],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_full():
    output = get_output("run -t something -f".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": True,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_full():
    output = get_output("compile -t something -f".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ['something'],
        "exclude": [],
        "profile": None,
        "full_load": True,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_exclude():
    output = get_output("run -x something".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": [],
        "exclude": ['something'],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_exclude():
    output = get_output("compile -x something".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": [],
        "exclude": ['something'],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_run_include_exclude():
    output = get_output("run -x something -t somethingelse".split(' '))

    assert output == {
        "command": "run",
        "debug": False,
        "include": ['somethingelse'],
        "exclude": ['something'],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }


def test_compile_include_exclude():
    output = get_output("compile -x something -t somethingelse".split(' '))

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ['somethingelse'],
        "exclude": ['something'],
        "profile": None,
        "full_load": False,
        "start_dt": yesterday,
        "end_dt": yesterday,
    }
