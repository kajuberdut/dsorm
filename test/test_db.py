import uuid

import pytest
from dsorm import *


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
    t = Table(
        name="dependant",
        column=[Column(name="SomeColumn"), Column(name="test_id")],
        constraints=[table_setup.fkey()],
    )


def test_init_db(db_path):
    Database(db_path=db_path).init_db()


def test_where_none():
    assert Where(None).sql() == ""


@pytest.fixture(scope="function")
def stuff():
    return str(uuid.uuid4())


def test_joinmap_single(table_setup):
    assert joinmap(table_setup) == table_setup.name


def test_db_insert_retreive_delete(table_setup, db, stuff):
    d = {"stuff": stuff}
    db.create("test", data=d)
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
    t = Statement.StatementOrder
    s = Statement(
        statement_type=t,
        components={t: "SELECT 1 as thing", t.WHERE: "WHERE 1=1"},
    )
    assert s.sql is not None


def test_no_default():
    with pytest.raises(TypeError):
        Database().connect()


def test_default(table_setup):
    Database.default_db = ":memory:"
    db = Database()
    db.query(table="test", columns=["1 as thing"])
    Database.default_db = None


def test_db_close():
    db = Database(":memory:")
    db.close()
