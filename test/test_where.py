import pytest
from dsorm import Where, ds_where


def test_where_none():
    assert Where({}).sql() == ""
    w = Where({1: 2})
    assert ds_where({1: 2}) == w
    assert ds_where(w) == w


def test_comparison():
    w = Where({"column_name": Where.get_comparison(target="thingy")})
    assert w.sql() == "WHERE [column_name] = 'thingy'"


def test_comparison_no_target():
    with pytest.raises(TypeError):
        w = Where.get_comparison()
        w.sql()


def test_comparison_no_column():
    with pytest.raises(TypeError):
        w = Where.get_comparison(target="thingy")
        w.sql()


def test_in():
    w = Where.is_in(column="value", target=[1, 2])
    assert w.sql() == "value  IN (1, 2)"


def test_in_no_target():
    with pytest.raises(TypeError):
        w = Where.is_in()
        w.sql()


def test_where_construction():
    w = Where()
    w[1] = 2
    w["this"] = "that"
    assert w[1] == 2
    assert list(w.items()) == [(1, 2), ("this", "that")]


def test_nested_where():
    w = Where(where={1: 1, "or": Where({1: 2})})
    assert w.sql() == "WHERE 1 = 1\nor ( 1 = 2\n)"
