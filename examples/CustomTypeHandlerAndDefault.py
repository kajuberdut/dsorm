from datetime import datetime
from uuid import UUID, uuid4

from dsorm import Column, Database, Table, TypeHandler


# Subclass TypeHandler to handle any python type
# Be sure your methods are static methods like the example
class UUIDHandler(TypeHandler):
    sql_type = "TEXT"  # If you don't set sql_type you will get whatever type affinity SQLite has for the value.
    python_type = UUID  # This should be a valid python type.

    @staticmethod
    def p2s(value):
        """ This method should return a value that would be valid "as is" in a sql statement. """
        return (
            f"'{str(value)}'"  # Note that strings need single quotes to be valid in SQL
        )

    @staticmethod
    def s2p(value):
        """ This method should handle converting a SQLite datatype to a Python datatype. """
        return UUID(value)


# Custom type handlers must be registered before use.
# This classmethod is inherited from TypeHandler
UUIDHandler.register()


# For another example of a default function see:
# https://github.com/kajuberdut/dsorm/blob/main/examples/PracticalExample.py
def default_uuid():
    return uuid4()


Person = Table(
    name="person",
    column=[
        Column("id", python_type=UUID, pkey=True, default=default_uuid),
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
        Column("create_date", python_type=datetime, default=datetime.now),
    ],
)


Database.default_db = ":memory:"
db = Database()
db.init_db()

db.insert("person", data={"first_name": "John", "last_name": "Doe"})
print(db.query("person"))
