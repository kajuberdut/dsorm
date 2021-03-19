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
        """ This static method should return a value that would be valid "as is" in a sql statement. """
        return (
            f"'{str(value)}'"  # Note that strings need single quotes to be valid in SQL
        )

    @staticmethod
    def s2p(value):
        """ This static method should handle converting a SQLite datatype to a Python datatype. """
        return UUID(value)


# Custom type handlers must be registered before use.
# This classmethod is inherited from TypeHandler
UUIDHandler.register()


# For another example of a default function see:
# https://github.com/kajuberdut/dsorm/blob/main/examples/PracticalExample.py
def default_uuid():
    return uuid4()


# DateHandler is built into dsORM
person = Table(
    table_name="person",
    column=[
        Column(column_name="id", python_type=UUID, pkey=True, default=default_uuid),
        Column(column_name="first_name", nullable=False),
        Column(column_name="last_name", nullable=False),
        Column(column_name="create_date", python_type=datetime, default=datetime.now),
    ],
)

Database.memory().init_db()

person.insert(data={"first_name": "John", "last_name": "Doe"}).execute()
print(person.select().execute())
