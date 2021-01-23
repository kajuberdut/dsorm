"""
This example is a WIP
"""

from dsorm import (
    Column,
    Cursor,
    Database,
    Pragma,
    Table,
    Where,
    post_connect,
    pre_connect,
)

# The pre_connect wrapper let's you set a function that will be called before the first connection
@pre_connect()
def db_setup(db):
    db.default_db = ":memory:"


# The post_connect wrapper is called once after the first connection is made
@post_connect()
def build(db):
    # This will set pragma and create all tables.
    # The objects are defined further down
    # Using this hook pushes instantiation to first connection
    db.init_db()


# Foreign key enforcement is off by default in SQLite
conf = Pragma(
    pragma={
        "foreign_keys": 1,
        "temp_store": 2,
    }
)