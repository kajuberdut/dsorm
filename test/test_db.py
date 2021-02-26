import uuid
from datetime import datetime

import pytest
from dsorm import (Column, Database, DateHandler, FloatHandler, ForeignKey,
                   IntHandler, Pragma, Statement, Table, TypeHandler,
                   TypeMaster, Where, ds_name, ds_where)


def test_db_setter():
    db = Database(":memory:")
    db.default_db = ":memory:"
    db.default_db = None


@pytest.fixture(scope="function")
def db_path():
    return ":memory:"


@pytest.fixture(scope="function")
def cur(db_path):
    db = Database(db_path=db_path)
    with db.cursor() as cur:
        yield cur


@pytest.fixture(scope="function")
def db(db_path):
    db = Database(db_path, is_default=True)
    yield db
    db.close()


@pytest.fixture(scope="function")
def table_setup(db):
    test_table = Table(
        name="test",
        column=[
            Column("test_id", python_type=int, pkey=True),
            Column("stuff", unique=True, nullable=False),
        ],
        _db=db,
    )
    db.execute(test_table.sql())
    return test_table


def test_name(table_setup):
    result = ds_name(table_setup)
    assert result.name == "test"


def test_column_repr():
    n = "Totally just a test"
    c = Column(name=n)
    assert repr(c) == f"[{n}]"


def test_column_from_tuple():
    c = Column.from_tuple(("test", str))
    assert c == Column(name="test", python_type=str)


def test_table_from_dict():
    t1 = Table.from_dict("TestTable", {"test": str})
    t2 = Table(name="TestTable", column=[Column(name="test", python_type=str)])
    assert t1.name == t2.name
    assert t1.column[0].name == t2.column[0].name
    assert t1.column[0].python_type == t2.column[0].python_type


def test_foreign_key():
    t1 = Table(name="footable", column=[Column.id()])
    t2 = Table(name="bartable", column=[Column(name="fooid", python_type=str)])
    f = ForeignKey(column=t1.column[0], reference=t2.column[0])
    print(f.sql())
    assert (
        f.sql()
        == "FOREIGN KEY ([footable].[id])\nREFERENCES [bartable]([bartable].[fooid])"
    )


def test_table_pkey(table_setup):
    assert repr(table_setup.pkey()[0]) == "[test].[test_id]"


def test_schema(table_setup):
    table_setup.schema = "main"
    assert table_setup.identity.sql() == "[main].[test]"
    assert repr(table_setup) == "[main].[test]"
    table_setup.schema = None


def test_init_db(db_path):
    p = Pragma.from_dict({"foreign_keys": 1})
    assert p[0].sql() == "PRAGMA foreign_keys=1"


def test_where_none():
    assert Where({}).sql() == ""
    w = Where({1: 2})
    assert ds_where({1: 2}) == w
    assert ds_where(w) == w


def test_comparison():
    w = Where({"column_name": Where.get_comparison(target="thingy")})
    assert w.sql() == "WHERE [column_name] = 'thingy'"


@pytest.fixture(scope="function")
def stuff():
    return str(uuid.uuid4())


def test_db_insert_retreive_delete(table_setup, db, stuff):
    d = {"stuff": stuff}
    table_setup.insert(data=d).execute()
    result = table_setup.select(where=d).execute()
    assert result[0]["stuff"] == stuff
    table_setup.delete(where=d).execute()
    result = table_setup.select(where=d).execute()
    assert len(result) == 0


def test_pragma():
    pragma = Pragma.from_dict(
        {
            "foreign_keys": 1,
            "temp_store": 2,
        }
    )
    for p in pragma:
        assert "=" in p.sql()


def test_statement():
    s = Statement(
        components={
            Statement.Order.BEGINNING: "SELECT 1 as thing",
            Statement.Order.END: "WHERE 1=1",
        },
    )
    assert s.sql is not None


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


def test_db_close():
    db = Database(":memory:")
    db.close()


def test_IntHandler():
    assert IntHandler.p2s(1) == "1"
    assert IntHandler.s2p("1") == 1


def test_FloatHandler():
    assert FloatHandler.p2s(1.0) == "1.0"
    assert FloatHandler.s2p("1.0") == 1.0


def test_DateHandler():
    d = datetime.now()
    assert DateHandler.p2s(d) == str(d.timestamp())
    assert DateHandler.s2p(d.timestamp()) == d


class NoneClass:
    ...


def test_custom_handler():
    class NoneMaker(TypeHandler):
        python_type = NoneClass
        sql_type = ""

        @staticmethod
        def p2s(value):
            return "NULL"

        @staticmethod
        def s2p(value):
            return None

    NoneMaker.register()
    assert TypeMaster()[NoneClass] == NoneMaker

    t = Table(
        name="NoneTable", column=[Column(name="NoneColumn", python_type=NoneClass)]
    )

    Database(db_path=":memory:", is_default=True).init_db()
    t.insert(data={"NoneColumn": NoneClass()})
    t.select(column=["NoneColumn", "1 as thing"])
    t.select(column=["NoneColumn", "1 as thing"])


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
