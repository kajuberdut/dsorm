from unittest import main

from dsorm import Column, ForeignKey, Qname, Table, ds_name

from .db_mixin import DB


class TestSchema(DB):
    def test_name(self):
        result = ds_name(self.table_setup)
        self.assertEqual(result, "test")

    def test_column_repr(self):
        n = "Totally just a test"
        c = Column(column_name=n)
        self.assertEqual(repr(c), f"[{n}]")

    def test_column_from_tuple(self):
        c = Column.from_tuple(("test", str))
        self.assertEqual(c, Column(column_name="test", python_type=str))

    def test_table_from_object(self):
        t1 = Table.from_object({"table_name": "TestTable", "test": str})
        t2 = Table(
            table_name="TestTable", column=[Column(column_name="test", python_type=str)]
        )
        self.assertEqual(t1.name, t2.name)
        self.assertEqual(t1.column[0].name, t2.column[0].name)
        self.assertEqual(t1.column[0].python_type, t2.column[0].python_type)

    def test_foreign_key(self):
        t1 = Table(table_name="footable", column=[Column.id()])
        t2 = Table(
            table_name="bartable",
            column=[Column(column_name="fooid", python_type=str, pkey=True)],
        )
        f = ForeignKey(column=t1.column[0], reference=t2.column[0])
        self.assertEqual(f.sql(), "FOREIGN KEY ( id ) REFERENCES bartable ( fooid )")

    def test_table_pkey(self):
        self.assertEqual(repr(self.table_setup.pkey()), "[test].[test_id]")

    def test_schema(self):
        table = self.table_setup
        table.schema_name = "main"
        self.assertEqual(table.identity.sql(), "[main].[test]")
        self.assertEqual(repr(table), "[main].[test]")
        table.schema = None

    def test_table_fkey(self):
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
        self.assertEqual(
            book.fkey(on_column=(Column(column_name="bob", table=BobsTable))).sql(),
            "FOREIGN KEY ( bob ) REFERENCES some_table ( id )",
        )
        BobsTable.constraint_sql()
        self.assertEqual(
            BobsTable.components[Table.Order["CONSTRAINT"]],
            ",FOREIGN KEY ( bob ) REFERENCES some_table ( id )",
        )

    def test_bad_table(self):
        with self.assertRaises(ValueError):
            Table(table_name="")

    def test_bad_column(self):
        with self.assertRaises(ValueError):
            Column()

    def test_column_hash(self):
        self.assertEqual(hash(Column(column_name="bob")), hash((None, "bob")))


if __name__ == "__main__":
    main()  # pragma: no cover
