import uuid

import pytest
from dsorm import Column, Database, Table


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(scope="function")
def stuff():
    return str(uuid.uuid4())


@pytest.fixture(scope="function")
def db_path():
    return ":memory:"


@pytest.fixture(scope="function")
def cur(db_path):
    db = Database(db_path=db_path)
    with db.cursor() as cur:
        yield cur


@pytest.fixture(scope="function")
def db(db_path):
    db = Database(db_path, is_default=True)
    yield db
    db.close()


@pytest.fixture(scope="function")
def table_setup(db):
    test_table = Table(
        table_name="test",
        column=[
            Column(column_name="test_id", python_type=int, pkey=True),
            Column(column_name="stuff", unique=True, nullable=False),
        ],
        _db=db,
    )
    db.execute(test_table.sql())
    return test_table
