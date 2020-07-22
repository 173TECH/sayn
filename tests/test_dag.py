import pytest

from sayn.utils import dag


def test_valid01():
    test_dag = {"a": ["b", "c"], "b": [], "c": []}
    assert dag.is_valid(test_dag)


def test_valid02():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert dag.is_valid(test_dag)


def test_missing_parents01():
    test_dag = {"a": ["b", "c"], "b": ["c", "adf"], "c": ["d"]}
    with pytest.raises(dag.MissingParentsError):
        dag.is_valid(test_dag)


def test_cycle01():
    test_dag = {"a": ["b", "c"], "b": ["b"], "c": list()}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_cycle02():
    test_dag = {"a": ["b", "c"], "b": ["a"], "c": []}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_cycle03():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": ["a"]}
    with pytest.raises(dag.CycleError):
        dag.is_valid(test_dag)


def test_topological_sort01():
    test_dag = {"a": [], "b": [], "c": []}
    assert dag.topological_sort(test_dag) == ["a", "b", "c"]


def test_topological_sort02():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert dag.topological_sort(test_dag) == ["c", "b", "a"]


def test_query01():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert set(dag.upstream(test_dag, "a")) == set(["c", "b"])


def test_query02():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert dag.downstream(test_dag, "a") == []


def test_query03():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert dag.downstream(test_dag, "b") == ["a"]


def test_query04():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert set(dag.downstream(test_dag, "c")) == set(["b", "a"])


def test_query05():
    test_dag = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert dag.upstream(test_dag, "c") == []
