from sqlite3.dbapi2 import OperationalError

import pytest
from dsorm import Database, Pragma


def test_db_setter(db_path):
    db = Database(db_path)
    db.default_db = db_path
    db.default_db = None


def test_none_path():
    with pytest.raises(TypeError):
        Database().connect()


def test_initialize():
    p = Pragma.from_dict({"foreign_keys": 1})
    assert p[0].sql() == "PRAGMA foreign_keys=1"


def test_pragma():
    pragma = Pragma.from_dict(
        {
            "foreign_keys": 1,
            "temp_store": 2,
        }
    )
    for p in pragma:
        assert "=" in p.sql()


def test_db_close(db_path):
    db = Database(db_path)
    db.close()


def test_execute(db_path):
    result = Database(db_path).execute("select 1 as stuff")
    assert result[0]["stuff"] == 1


def test_extest_bad_executeecute(db_path):
    with pytest.raises(OperationalError):
        Database(db_path).execute("1")  # type: ignore
