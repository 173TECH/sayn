import pytest

from sayn.utils import dag


def test_valid01():
    test_dag = {"task1": ["task2", "task3"], "task2": [], "task3": []}
    assert dag.is_valid(test_dag)


def test_valid02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.is_valid(test_dag)


def test_missing_parents01():
    test_dag = {
        "task1": ["task2", "task3"],
        "task2": ["task3", "adf"],
        "task3": ["task4"],
    }
    with pytest.raises(dag.MissingParentsError):
        dag.is_valid(test_dag)


def test_cycle01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task2"], "task3": []}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_cycle02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task1"], "task3": []}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_cycle03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": ["task1"]}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_topological_sort01():
    test_dag = {"task1": [], "task2": [], "task3": []}
    assert dag.topological_sort(test_dag) == ["task1", "task2", "task3"]


def test_topological_sort02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.topological_sort(test_dag) == ["task3", "task2", "task1"]


def test_upstream01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert set(dag._upstream(test_dag, "task1")) == set(["task3", "task2"])


def test_downstream02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.downstream(test_dag, "task1") == []


def test_downstream03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.downstream(test_dag, "task2") == ["task1"]


def test_downstream04():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert set(dag.downstream(test_dag, "task3")) == set(["task2", "task1"])


def test_upstream05():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag._upstream(test_dag, "task3") == []


def test_query00():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(test_dag) == ["task3", "task2", "task1"]


def test_query01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": False,
                "upstream": False,
            }
        ],
    ) == ["task3"]


def test_query02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": True,
                "upstream": False,
            }
        ],
    ) == ["task3", "task2", "task1"]


def test_query03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": False,
                "upstream": True,
            }
        ],
    ) == ["task3"]


def test_query04():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": True,
                "upstream": True,
            }
        ],
    ) == ["task3", "task2", "task1"]


def test_cycle05():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task2"], "task3": []}
    with pytest.raises(dag.CycleError):
        dag.query(
            test_dag,
            [
                {
                    "operation": "include",
                    "task": "task3",
                    "downstream": True,
                    "upstream": True,
                }
            ],
        )
