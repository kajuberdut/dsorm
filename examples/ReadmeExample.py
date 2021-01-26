from dsorm import Column, Database, Table, Where

Person = Table(
    name="person",
    column=[
        Column.id(),  # This is shorthand for Column("id", int, pkey=True)
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
    ],
)

# Database instances can access any table with insert, query, or delete.
Database.default_db = ":memory:"
db = Database()
db.init_db()  # This creates all tables

# Insert records
db.insert(
    table="person",
    data=[
        {"first_name": "Jane", "last_name": "Doe"},
        {"first_name": "John", "last_name": "Doe"},
    ],
)

# Query returns a list of dicts of rows matching the where
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

# And Delete
db.delete("person", where={"id": does[0]["id"]})
print([r["id"] for r in db.query("person")])
# [2]
