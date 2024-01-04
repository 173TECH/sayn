from datetime import date, timedelta, datetime

import click
from click.testing import CliRunner
import pytest

import sayn.cli as tcli

yesterday = date.today() - timedelta(days=1)


@click.group()
def cli():
    pass


@cli.command()
@tcli.click_run_options
def run(
    debug,
    tasks,
    exclude,
    upstream_prod,
    profile,
    full_load,
    start_dt,
    end_dt,
    n_threads,
):

    tasks = [i for t in tasks for i in t.strip().split(" ")]
    exclude = [i for t in exclude for i in t.strip().split(" ")]

    if start_dt is not None:
        start_dt = start_dt.date()
    if end_dt is not None:
        end_dt = end_dt.date()

    return {
        "command": "run",
        "debug": debug,
        "include": tasks,
        "exclude": exclude,
        "profile": profile,
        "full_load": full_load,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "n_threads": n_threads,
    }


@cli.command()
@tcli.click_run_options
def compile(
    debug,
    tasks,
    exclude,
    upstream_prod,
    profile,
    full_load,
    start_dt,
    end_dt,
    n_threads,
):

    tasks = [i for t in tasks for i in t.strip().split(" ")]
    exclude = [i for t in exclude for i in t.strip().split(" ")]

    if start_dt is not None:
        start_dt = start_dt.date()
    if end_dt is not None:
        end_dt = end_dt.date()

    return {
        "command": "compile",
        "debug": debug,
        "include": tasks,
        "exclude": exclude,
        "profile": profile,
        "full_load": full_load,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "n_threads": n_threads,
    }


def get_output(command):
    runner = CliRunner()
    result = runner.invoke(cli, command.split(" "), standalone_mode=False)
    output = result.return_value
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
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
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
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_one_task():
    output = get_output("run -t something")

    assert output == {
        "command": "run",
        "debug": False,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_two_tasks_old():
    output = get_output("run -t something -t somethingelse")

    assert output == {
        "command": "run",
        "debug": False,
        "include": ["something", "somethingelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_two_tasks_new():
    output = get_output("run -t something somethingelse")

    assert output == {
        "command": "run",
        "debug": False,
        "include": ["something", "somethingelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_one_task():
    output = get_output("compile -t something")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_two_tasks_old():
    output = get_output("compile -t something -t somethingelse")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ["something", "somethingelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_two_tasks_new():
    output = get_output("compile -t something somethingelse")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ["something", "somethingelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_debug():
    output = get_output("run -t something -d")

    assert output == {
        "command": "run",
        "debug": True,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_debug():
    output = get_output("compile -t something -d")

    assert output == {
        "command": "compile",
        "debug": True,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_debug_multitasks():
    output = get_output("run -t something -d -t somethingelse somesomeelse")

    assert output == {
        "command": "run",
        "debug": True,
        "include": ["something", "somethingelse", "somesomeelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_debug_multitasks():
    output = get_output("compile -t something -d -t somethingelse somesomeelse")

    assert output == {
        "command": "compile",
        "debug": True,
        "include": ["something", "somethingelse", "somesomeelse"],
        "exclude": [],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_full():
    output = get_output("run -t something -f")

    assert output == {
        "command": "run",
        "debug": False,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": True,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_full():
    output = get_output("compile -t something -f")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ["something"],
        "exclude": [],
        "profile": None,
        "full_load": True,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_exclude():
    output = get_output("run -x something")

    assert output == {
        "command": "run",
        "debug": False,
        "include": [],
        "exclude": ["something"],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_exclude():
    output = get_output("compile -x something")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": [],
        "exclude": ["something"],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_run_include_exclude():
    output = get_output("run -x something -t somethingelse")

    assert output == {
        "command": "run",
        "debug": False,
        "include": ["somethingelse"],
        "exclude": ["something"],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }


def test_compile_include_exclude():
    output = get_output("compile -x something -t somethingelse")

    assert output == {
        "command": "compile",
        "debug": False,
        "include": ["somethingelse"],
        "exclude": ["something"],
        "profile": None,
        "full_load": False,
        "start_dt": None,
        "end_dt": None,
        "n_threads": 1,
    }
