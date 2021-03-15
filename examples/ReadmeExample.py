from dsorm import ID_COLUMN, Column, Database, Table, Where

person = Table(
    table_name="person",
    column=[
        Column.id(),  # This is shorthand for Column("id", int, pkey=True)
        Column(column_name="first_name", python_type=str),
        Column(column_name="last_name", python_type=str),
    ],
)
print(person.sql())
# CREATE TABLE IF NOT EXISTS person (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT);

# Alternate way of declaring tables using a dictionary of columns
person2 = Table.from_object(
    table_name="person",
    object={
        "id": ID_COLUMN,
        "first_name": str,
        "last_name": str,
    },
)
assert person2.sql() == person.sql()

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
doe_family = person.select(
    where={"first_name": Where.like(target="J%n%")},
    column=[
        "id",
        "first_name || ' ' || last_name AS full_name",  # Note that the columns can be sql
    ],
).execute()

print(doe_family)
# [{"id": 1, "full_name": "John Doe"}, {"id": 2, "full_name": "Jane Doe"}]

# And Delete
person.delete(where={"id": doe_family[0]["id"]}).execute()
print([r["id"] for r in person.select().execute()])
# [2]
