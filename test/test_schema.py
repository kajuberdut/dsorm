import pytest
from dsorm import Column, ForeignKey, Table, ds_name, Qname


def test_name(table_setup):
    result = ds_name(table_setup)
    assert result == "test"


def test_column_repr():
    n = "Totally just a test"
    c = Column(column_name=n)
    assert repr(c) == f"[{n}]"


def test_column_from_tuple():
    c = Column.from_tuple(("test", str))
    assert c == Column(column_name="test", python_type=str)


def test_table_from_object():
    t1 = Table.from_object({"table_name": "TestTable", "test": str})
    t2 = Table(
        table_name="TestTable", column=[Column(column_name="test", python_type=str)]
    )
    assert t1.name == t2.name
    assert t1.column[0].name == t2.column[0].name
    assert t1.column[0].python_type == t2.column[0].python_type


def test_foreign_key():
    t1 = Table(table_name="footable", column=[Column.id()])
    t2 = Table(
        table_name="bartable",
        column=[Column(column_name="fooid", python_type=str, pkey=True)],
    )
    f = ForeignKey(column=t1.column[0], reference=t2.column[0])
    assert f.sql() == "FOREIGN KEY ( id ) REFERENCES bartable ( fooid )"


def test_table_pkey(table_setup):
    assert repr(table_setup.pkey()) == "[test].[test_id]"


def test_schema(table_setup):
    table_setup.schema_name = "main"
    assert table_setup.identity.sql() == "[main].[test]"
    assert repr(table_setup) == "[main].[test]"
    table_setup.schema = None


def test_table_fkey():
    book = Table(
        table_name="some_table",
        column=[
            Column(column_name="id", python_type=int, pkey=True),
            Column(column_name="some_other_id", python_type=int),
        ],
    )
    BobsTable = Table(
        table_name="BobsTable",
        column=[Column(column_name="bob")],
        constraints=[
            book.fkey(on_column=(Qname(column_name="bob", table_name="BobsTable")))
        ],
    )
    assert (
        book.fkey(on_column=(Column(column_name="bob", table=BobsTable))).sql()
        == "FOREIGN KEY ( bob ) REFERENCES some_table ( id )"
    )
    BobsTable.constraint_sql()
    assert (
        BobsTable.components[Table.Order["CONSTRAINT"]]
        == ",FOREIGN KEY ( bob ) REFERENCES some_table ( id )"
    )


def test_bad_table():
    with pytest.raises(ValueError):
        Table(table_name="")


def test_bad_column():
    with pytest.raises(ValueError):
        Column()


def test_column_hash():
    assert hash(Column(column_name="bob")) == hash((None, "bob"))
