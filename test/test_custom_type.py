from datetime import datetime

import pytest
from dsorm import (Column, Database, DateHandler, FloatHandler, IntHandler,
                   Table, TypeHandler, TypeMaster)


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
        table_name="NoneTable", column=[Column(column_name="NoneColumn", python_type=NoneClass)]
    )

    Database(db_path=":memory:", is_default=True).init_db()
    t.insert(data={"NoneColumn": NoneClass()})
    t.select(column=["NoneColumn", "1 as thing"])
    t.select(column=["NoneColumn", "1 as thing"])


def test_bad_register():
    class DumbHandler(TypeHandler):
        pass
    with pytest.raises(TypeError):
        DumbHandler.register()
