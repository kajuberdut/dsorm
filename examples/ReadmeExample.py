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

Person = Table(
    name="person",
    column=[
        Column("id", python_type=int, pkey=True),
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
        Column("screen_name", unique=True),
    ],
)

# Table objects have select, insert, and delete methods
stmt = Person.insert(data={"first_name": "John", "last_name": "Doe"})

with Cursor() as cur:
    cur.execute(stmt.sql())
    print(cur.execute(Person.select()))

# Database instances can access any table with Create, Query, or Delete.
db = Database()

# Inserts a record
db.create(table="person", data={"first_name": "Jane", "last_name": "Doe"})

# Select a list of rows matching the where
johns = db.query(
    "person",
    where={"first_name": Where.is_in(target=["John", "Jane"])},
    columns=[
        "id",
        "first_name || ' ' || last_name AS full_name",  # Note that the columns can be sql
    ],
)
print(johns)

db.delete("person", where={"id": johns[0]["id"]})
print([r["id"] for r in db.query("person")])
