import uuid
from datetime import datetime

import pytest
from dsorm import (Column, Cursor, Database, DateHandler, FloatHandler,
                   ForeignKey, IntHandler, Pragma, Statement, Table,
                   TypeHandler, TypeMaster, Where, ds_name, ds_quote, ds_where,
                   joinmap)


def test_db_setter():
    db = Database(":memory:")
    db.default_db = ":memory:"
    db.default_db = None


@pytest.fixture(scope="session")
def db_path():
    return ":memory:"


@pytest.fixture(scope="function")
def cur(db_path):
    with Cursor(db_path) as cur:
        yield cur


@pytest.fixture(scope="session")
def db(db_path):
    return Database(db_path)


@pytest.fixture(scope="session")
def table_setup(db_path):
    with Cursor(db_path=db_path) as cur:
        test_table = Table(
            name="test",
            column=[
                Column("test_id", python_type=int, pkey=True),
                Column("stuff", unique=True, nullable=False),
            ],
        )
        cur.execute(test_table.sql())
    return test_table


def test_name(table_setup):
    assert ds_name(table_setup) == "test"


def test_column_repr():
    n = "Totally just a test"
    c = Column(name=n)
    assert repr(c) == f"[{n}]"


def test_foreign_key():
    f = ForeignKey("test_column", "test", "id")
    assert f.name == "Fkey on test_column"
    assert f.sql() == "FOREIGN KEY (test_column) REFERENCES test(id)"
    assert repr(f) == "FKEY test(id)"


def test_table_pkey(table_setup):
    assert repr(table_setup.pkey()[0]) == "[test].[test_id]"


def test_schema(table_setup):
    table_setup.schema = "main"
    assert table_setup.identifier == "main.test"
    assert repr(table_setup) == "main.test(test_id, stuff)"
    table_setup.schema = None


def test_table_fkey(table_setup):
    Table(
        name="dependant",
        column=[Column(name="SomeColumn"), Column(name="test_id")],
        constraints=[table_setup.fkey()],
    )


def test_init_db(db_path):
    Database(db_path=db_path).init_db()


def test_where_none():
    assert Where(None).sql() == ""
    w = Where({1: 2})
    assert ds_where({1: 2}) == w
    assert ds_where(w) == w


def test_comparison():
    w = Where({"column_name": Where.get_comparison(target="thingy")})
    assert w.sql() == "WHERE [column_name] = 'thingy'"


def test_cast():
    assert ds_quote(Column(name="bob")) == "[bob]"


@pytest.fixture(scope="function")
def stuff():
    return str(uuid.uuid4())


def test_joinmap_single(table_setup):
    assert joinmap(table_setup) == table_setup.name


def test_joinmap_int():
    assert joinmap([1, 2, 3]) == "1, 2, 3"


def test_db_insert_retreive_delete(table_setup, db, stuff):
    d = {"stuff": stuff}
    db.insert("test", data=d)
    result = db.query("test", where=d)
    assert result[0]["stuff"] == stuff
    db.delete("test", where=d)
    result = db.query(table_setup, where=d)
    assert len(result) == 0


def test_pragma():
    pragma = {
        "foreign_keys": 1,
        "temp_store": 2,
    }
    p = Pragma(pragma=pragma)
    assert len(p.sql()) > len("".join(pragma.keys()))


def test_statement():
    s = Statement(
        components={
            Statement.Order.SELECT: "SELECT 1 as thing",
            Statement.Order.WHERE: "WHERE 1=1",
        },
    )
    assert s.sql is not None


def test_no_default():
    with pytest.raises(TypeError):
        Database().connect()


def test_comparison_no_target():
    with pytest.raises(TypeError):
        w = Where.get_comparison()
        w.sql()


def test_comparison_no_column():
    with pytest.raises(TypeError):
        w = Where.get_comparison(target="thingy")
        w.sql()


def test_in():
    w = Where.is_in(target=[])
    w.sql()


def test_in_no_target():
    with pytest.raises(TypeError):
        w = Where.is_in()
        w.sql()


def test_default(table_setup):
    Database.default_db = ":memory:"
    db = Database()
    db.default_db = ":memory:"
    db.query(table="test", columns=["1 as thing"])
    Database.default_db = None


def test_db_close():
    db = Database(":memory:")
    db.close()


def test_TypeHandler():
    assert TypeHandler.p2s("bob") == "bob"
    assert TypeHandler.s2p("bob") == "bob"


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

    TypeMaster.register(NoneMaker)
    assert TypeMaster()[NoneClass] == NoneMaker

    t = Table(
        name="NoneTable", column=[Column(name="NoneColumn", python_type=NoneClass)]
    )

    db = Database(db_path=":memory:")
    db.init_db()
    assert db.table(t) == t
    db.insert("NoneTable", data={"NoneColumn": NoneClass()})
    db.query("NoneTable", columns=["NoneColumn", "1 as thing"])
    db.query("NoneTable", columns=["NoneColumn", "1 as thing"], cast_values=False)


def test_static_default():
    assert Column(name="bob", default="bob").sql() == "bob TEXT DEFAULT 'bob'"


def test_paramters():
    with Cursor(db_path=":memory:") as cur:
        cur.execute("SELECT ? as thing", (1,))


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
