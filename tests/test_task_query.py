import pytest

from sayn.utils.task_query import get_query


tasks = {
    "task1": {"task_group": "group1", "tags": list()},
    "task2": {"task_group": "group1", "tags": ["tag1"]},
    "task3": {"task_group": "group2", "tags": ["tag1"]},
    "task4": {"task_group": "group2", "tags": list()},
    "task5": {"task_group": "group3", "tags": ["tag1", "tag2"]},
    "task6": {"task_group": "group3", "tags": list()},
    "task7": {"task_group": "group3", "tags": list()},
}


def test_simple01():
    assert get_query(tasks, include=["task1"]).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        }
    ]


def test_simple02():
    assert get_query(tasks, include=["task_undefined"]).is_err


def test_simple03():
    assert get_query(tasks, include=["task2+"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": True,
        }
    ]


def test_simple04():
    assert get_query(tasks, include=["+task4+"]).value == [
        {"operation": "include", "task": "task4", "upstream": True, "downstream": True}
    ]


def test_group01():
    assert get_query(tasks, include=["group:group1"]).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_tag01():
    assert get_query(tasks, include=["tag:tag1"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_error_identifier01():
    assert get_query(tasks, include=["+_task_undefined"]).is_err


def test_error_identifier02():
    assert get_query(tasks, include=["tag:tag_undefined"]).is_err


def test_error_identifier03():
    assert get_query(tasks, include=["group:group_undefined"]).is_err


def test_full_query02():
    assert get_query(tasks, include=["tag:tag1"], exclude=["group:group2"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task4",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_full_query03():
    assert get_query(
        tasks,
        include=["task1", "task2+", "tag:tag1", "+task2"],
        exclude=["group:group3", "+task3"],
    ).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task2",
            "upstream": True,
            "downstream": True,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task6",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task7",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task3",
            "upstream": True,
            "downstream": False,
        },
    ]
