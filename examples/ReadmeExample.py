from dsorm import (
    Column,
    Cursor,
    Database,
    Table,
    Where,
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

# Database instances can access any table with insert, query, or delete.
Database.default_db = ":memory:"
db = Database()
db.init_db()  # This creates all tables

# Inserts a record
db.insert(
    table="person",
    data=[
        {"first_name": "Jane", "last_name": "Doe"},
        {"first_name": "John", "last_name": "Doe"},
    ],
)

# Query returns a list of dicts representing rows matching the where
does = db.query(
    "person",
    where={"first_name": Where.like(target="J%n%")},
    columns=[
        "id",
        "first_name || ' ' || last_name AS full_name",  # Note that the columns can be sql
    ],
)
print(does)
# [{"id": 1, "full_name": "John Doe"}, {"id": 2, "full_name": "Jane Doe"}]

db.delete("person", where={"id": does[0]["id"]})
print([r["id"] for r in db.query("person")])
# [2]
