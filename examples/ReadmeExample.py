from dsorm import ID_COLUMN, Column, Database, Table, Where

person = Table(
    name="person",
    column=[
        Column.id(),  # This is shorthand for Column("id", int, pkey=True)
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
    ],
)

print(person.sql())


person2 = Table.from_dict(
    "person",
    {
        "id": ID_COLUMN,
        "first_name": {"nullable": False},
        "last_name": {"nullable": False},
    },
)

print(person2.sql())

# See Database example for more details about the Database object
Database(db_path=":memory:", is_default=True).init_db()  # This creates all tables


# Tables have insert, select, and delete methods.
# These return a Statement
stmt = person.insert(
    data=[
        {"first_name": "Jane", "last_name": "Doe"},
        {"first_name": "John", "last_name": "Doe"},
    ],
)

# Statements can be examined with .sql method
print(stmt.sql())

# INSERT INTO Main.person (first_name, last_name)
# VALUES ('Jane', 'Doe'), ('John', 'Doe')

# or executed with .execute()
stmt.execute()

# select returns a list of dicts of rows matching the where
does = person.select(
    where={"first_name": Where.like(target="J%n%")},
    column=[
        "id",
        "first_name || ' ' || last_name AS full_name",  # Note that the columns can be sql
    ],
).execute()

print(does)
# [{"id": 1, "full_name": "John Doe"}, {"id": 2, "full_name": "Jane Doe"}]

# And Delete
person.delete(where={"id": does[0]["id"]}).execute()
print([r["id"] for r in person.select().execute()])
# [2]
