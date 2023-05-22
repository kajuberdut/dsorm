from dsorm.db_objects import Table, Column, FKey

"""
CREATE TABLE people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    address_id INTEGER,
    FOREIGN KEY (address_id) REFERENCES address(id)
);

CREATE INDEX idx_people_email ON people(email);
"""

id_column = Column.primary_key("id")
name_column = Column("name", str, "NOT NULL")
age_column = Column("age", int)

table = Table("users", id_column, name_column, age_column, schema="public")
print(table)

user_id_column = Column("user_id", int, constraints=FKey(references=id_column))
table2 = Table("user_profiles", user_id_column, schema="public")
print(table2)
