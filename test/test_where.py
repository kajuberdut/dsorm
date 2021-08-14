import pytest
from dsorm import Where, Comparison
from dsorm.dsorm import columnify


def test_comparison():
    w = Where({"column_name": Comparison.get_comparison(target="thingy", key="thingy")})
    assert w.sql() == "WHERE [column_name] = :thingy"


def test_comparison_no_target():
    with pytest.raises(TypeError):
        w = Comparison.get_comparison()
        w.sql()


def test_comparison_no_column():
    with pytest.raises(TypeError):
        w = Comparison.get_comparison(target="thingy")
        w.sql()


def test_in():
    w = Comparison.is_in(column="value", target=[1, 2])
    assert w.sql() == "value  IN (1, 2)"


def test_in_no_target():
    with pytest.raises(TypeError):
        w = Comparison.is_in()
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
    w = Where(where={column_a: Comparison.eq(target=BOOK_NAME, key="BookName"), "or": Where({column_b: Comparison.eq(target=AUTHOR_NAME, key="AuthorName")})})
    assert (
        w.sql()
        == "WHERE [book].[name] = :BookName or ([author].[name] = :AuthorName)"
    )
