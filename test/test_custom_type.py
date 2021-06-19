from datetime import datetime

import pytest
from dsorm import Column, Database, DateHandler, Table, TypeHandler, TypeMaster


def test_DateHandler():
    d = datetime.now()
    assert DateHandler.to_sql(d) == str(d.timestamp())
    assert DateHandler.to_python(d.timestamp()) == d


class NoneClass:
    ...


def test_custom_handler():
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
    assert TypeMaster()[NoneClass] == NoneMaker

    t = Table(
        table_name="NoneTable",
        column=[Column(column_name="NoneColumn", python_type=NoneClass)],
    )

    Database.memory().initialize()
    t.insert(data={"NoneColumn": NoneClass()})
    t.select(column=["NoneColumn", "1 as thing"])
    t.select(column=["NoneColumn", "1 as thing"])


def test_bad_register():
    class DumbHandler(TypeHandler):
        pass

    with pytest.raises(TypeError):
        DumbHandler.register()
