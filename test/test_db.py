from dso import Database

import pytest


@pytest.fixture(scope="function")
def memory_db():
    with Database(":memory:") as db:
        yield db


def test_db_create(memory_db):
    memory_db.execute("CREATE TABLE test(n INTEGER)")
    memory_db.commit()


def test_db_rollback(memory_db):
    memory_db.execute("INSERT INTO test(n) VALUES(:int_value)", {"int_value": 1})
    memory_db.rollback()


def test_db_close(memory_db):
    memory_db.close()


def test_db_no_default():
    restore = Database.default_db
    Database.clear_default_db()
    with pytest.raises(ValueError):
        with Database() as d:
            d.execute("SELECT 1")
    Database.set_default_db(restore)


def test_db_default():
    restore = Database.default_db
    Database.set_default_db(":memory:")
    with Database() as d:
        read_def = d.default_db
        assert read_def == ":memory:"
    Database.set_default_db(restore)
