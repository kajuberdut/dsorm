from dsorm.db_objects import Column, FKey, Table
import dsorm

dsorm.default_schema = "public"


addresses_table = Table(
    "addresses",
    Column.primary_key(),
    # This classmethod is equivilent to
    #   Column( column_name="id"
    #         , python_type=int
    #         , inline_constraints=dialect specific primary key decleration
    #         )
    Column("street"),
    Column("city"),
    Column("state"),
    Column("postal_code"),
)


user_table = Table(
    "users",
    Column.primary_key(),
    Column("name"),
    Column("address_id", constraints=FKey(references=addresses_table["id"])),
)
print(addresses_table)

"""CREATE TABLE addresses (
    id INT PRIMARY KEY,
    street TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT NOT NULL
);
"""
print(user_table)

"""CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    address_id TEXT NOT NULL,
    FOREIGN KEY (address_id) REFERENCES addresses(id)
);"""