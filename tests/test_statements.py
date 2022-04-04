import uuid
from unittest import main

from dsorm import Column, Database, Insert, Qname, Statement, Table
from dsorm.dsorm import columnify

from .db_mixin import DB


class TestStatements(DB):
    db_path = ":memory:"

    def test_static_default(self):
        self.assertEqual(
            Column(column_name="bob", default="'bob'").sql(), "bob TEXT DEFAULT 'bob'"
        )

    def data_func(self, data):
        return "Value1"

    def no_arg_func(self):
        return "Value2"

    def test_insert_defaults(self):
        t = Table(
            table_name="NoneTable",
            column=[
                Column(column_name="Some Column", python_type=str),
                Column(column_name="datafunc", python_type=str, default=self.data_func),
                Column(
                    column_name="noargfunc", python_type=str, default=self.no_arg_func
                ),
            ],
        )
        ins = t.insert(data={"Some Column": "stuff"})
        self.assertEqual("Value1", ins.data[0]["datafunc"])
        self.assertEqual("Value2", ins.data[0]["noargfunc"])

    def test_insert_only_defaults(self):
        t = Table(
            table_name="NoneTable",
            column=[
                Column(column_name="Some Column", python_type=str),
                Column(column_name="datafunc", python_type=str, default=self.data_func),
                Column(
                    column_name="noargfunc", python_type=str, default=self.no_arg_func
                ),
            ],
        )
        self.assertIn("DEFAULT VALUES", t.insert(data=None).sql())

    def test_db_insert_retrieve_delete(self):
        stuff, table_setup = str(uuid.uuid4()), self.table_setup
        d = {"stuff": stuff}
        table_setup.insert(data=d).execute()
        result = table_setup.select(where=d).execute()
        self.assertEqual(result[0]["stuff"], stuff)
        table_setup.delete(where=d).execute()
        result = table_setup.select(where=d).execute()
        self.assertEqual(len(result), 0)

    def test_statement(self):
        s = Statement(
            components={
                Statement.Order.BEGINNING: "SELECT 1 as thing",
                Statement.Order.END: "WHERE 1=1",
            },
        )
        self.assertIsNotNone(s.sql)
        self.assertEqual(s[Statement.Order.BEGINNING], "SELECT 1 as thing")
        self.assertEqual(s[3], "WHERE 1=1")
        s[2] = " "
        self.assertEqual(s[2], " ")
        with self.assertRaises(ValueError):
            s[None] = None

    def test_set_db_after(self):
        s = Insert()
        s.db = Database(self.db_path)

    def test_qname(self):
        q = Qname(schema_name="main", table_name="thing")
        self.assertEqual(q.name, "thing")
        self.assertEqual(q.sql(), "[main].[thing]")

    def test_columnify(self):
        TABLE_NAME = "BobsTable"
        COLUMN_NAME = "bob"
        t = Table(table_name=TABLE_NAME, column=[Column.from_tuple((COLUMN_NAME, str))])
        c = t.column[0]
        q = columnify(f"{TABLE_NAME}.{COLUMN_NAME}")
        self.assertEqual(c.identity.sql(), q.sql())

    def test_bad_columnify(self):
        with self.assertRaises(ValueError):
            columnify(1)


if __name__ == "__main__":
    main()  # pragma: no cover
