from dsorm.dsorm import columnify
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

    AUTHOR_NAME = "JK Rowling"
    BOOK_NAME = "Harry Potter"
    column_a = columnify("book.name")
    column_b = columnify("author.name")
    w = Where(where={column_a: BOOK_NAME, "or": Where({column_b: AUTHOR_NAME})})
    assert (
        w.sql()
        == f"WHERE [book].[name] = '{BOOK_NAME}'\nor ( [author].[name] = '{AUTHOR_NAME}'\n)"
    )
