from datetime import datetime
from unittest import TestCase, main

from dsorm import Column, Database, DateHandler, Table, TypeHandler, TypeMaster


class NoneClass:
    ...


class TestCustomType(TestCase):
    def test_DateHandler(self):
        d = datetime.now()
        self.assertEqual(DateHandler.to_sql(d), str(d.timestamp()))
        self.assertEqual(DateHandler.to_python(d.timestamp()), d)

    def test_custom_handler(self):
        class NoneMaker(TypeHandler):
            python_type = NoneClass
            sql_type = ""

            @staticmethod
            def to_sql(value):
                return "NULL"

            @staticmethod
            def to_python(value):
                return None

        NoneMaker.register()
        self.assertEqual(TypeMaster()[NoneClass], NoneMaker)

        t = Table(
            table_name="NoneTable",
            column=[Column(column_name="NoneColumn", python_type=NoneClass)],
        )

        Database.memory().initialize()
        t.insert(data={"NoneColumn": NoneClass()})
        t.select(column=["NoneColumn", "1 as thing"])
        t.select(column=["NoneColumn", "1 as thing"])

    def test_bad_register(self):
        class DumbHandler(TypeHandler):
            pass

        with self.assertRaises(TypeError):
            DumbHandler.register()


if __name__ == "__main__":
    main()  # pragma: no cover
