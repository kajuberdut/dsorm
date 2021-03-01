import pytest
from dsorm import Column, Database, Insert, Qname, Statement, Table
from dsorm.dsorm import Registered


def test_static_default():
    assert Column(name="bob", default="bob").sql() == "bob TEXT DEFAULT 'bob'"


def data_func(data):
    return "Value1"


def no_arg_func():
    return "Value2"


def test_insert_defaults():
    t = Table(
        name="NoneTable",
        column=[
            Column(name="Some Column", python_type=str),
            Column(name="datafunc", python_type=str, default=data_func),
            Column(name="noargfunc", python_type=str, default=no_arg_func),
        ],
    )
    s = t.insert(data={"Some Column": "stuff"}).sql()
    assert "Value1" in s
    assert "Value2" in s


def test_insert_only_defaults():
    t = Table(
        name="NoneTable",
        column=[
            Column(name="Some Column", python_type=str),
            Column(name="datafunc", python_type=str, default=data_func),
            Column(name="noargfunc", python_type=str, default=no_arg_func),
        ],
    )
    assert "DEFAULT VALUES" in t.insert(data=None).sql()


def test_db_insert_retreive_delete(table_setup, stuff):
    d = {"stuff": stuff}
    table_setup.insert(data=d).execute()
    result = table_setup.select(where=d).execute()
    assert result[0]["stuff"] == stuff
    table_setup.delete(where=d).execute()
    result = table_setup.select(where=d).execute()
    assert len(result) == 0


def test_statement():
    s = Statement(
        components={
            Statement.Order.BEGINNING: "SELECT 1 as thing",
            Statement.Order.END: "WHERE 1=1",
        },
    )
    assert s.sql is not None
    assert s[Statement.Order.BEGINNING] == "SELECT 1 as thing"
    assert s[3] == "WHERE 1=1"
    s[2] = " "
    assert s[2] == " "
    with pytest.raises(ValueError):
        s[None] = None


def test_set_db_after(db_path):
    s = Insert()
    s.db = Database(db_path)


def test_noname_registered_object():
    with pytest.raises(ValueError) as e:
        Registered()
        assert "name must be set to register object" in e


def test_qname():
    q = Qname(parts=["main", "thing"])
    assert q.name == "thing"
    assert q.sql() == "[main].[thing]"
