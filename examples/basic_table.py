from dsorm.db_objects import Column, FKey, Table, Index

address_table = Table(
    "address",
    Column.primary_key(),
    Column("street"),
    Column("city"),
    Column("state"),
    Column("postal_code"),
)

idx_address_postal_code = Index(address_table["postal_code"])


user_table = Table(
    "user",
    Column.primary_key(),
    Column("name"),
    Column("address_id", constraints=FKey(references=address_table["id"])),
)


print(address_table, idx_address_postal_code)
"""CREATE TABLE IF NOT EXISTS addresses (
    id INT PRIMARY KEY,
    street TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT NOT NULL
);
"""

print(idx_address_postal_code)
"""CREATE  INDEX IF NOT EXISTS idx_address_postal_code ON address(postal_code)"""

print(user_table)
"""CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    address_id TEXT NOT NULL,
    FOREIGN KEY (address_id) REFERENCES addresses(id)
);"""
