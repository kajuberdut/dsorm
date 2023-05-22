import sqlite3

import dsorm
from dsorm.db_objects import Column, Table

# set dialect
dsorm.set_dialect("SQLITE")

# +-------------------------------------------------+
# |                PRE EXAMPLE SETUP                |
# +-------------------------------------------------+

user_table = Table(
    "user",
    Column.primary_key(),
    Column("first_name"),
    Column("last_name"),
    Column("email", inline_constraints=" UNIQUE NOT NULL"),
)


# Table must exist before the @sqlite_dbclass is created.
db = sqlite3.connect(":memory:")
dsorm.sqlite_execute(db, user_table)

# +-------------------------------------------------+
# |                EXAMPLE STARTS HERE              |
# +-------------------------------------------------+


# The db class decorator takes at minimum an instance of databases.Database
@dsorm.dblite(db=db)
class User:
    pass


# data is added as an init keyword
new_user = User(
    data={
        "first_name": "John",
        "last_name": "Doe",
        "email": "example@gmail.com",
    }
).save()

print(f"new_user isinstant of DBClass: {isinstance(new_user, dsorm.DBClass)}")

# load the same user from the db
loaded_user = User.load(new_user.id)

print(f"{loaded_user.id=}")
print(f"full name: {loaded_user.first_name} {loaded_user.last_name}")
print(f"{loaded_user.email=}")


# +-------------------------------------------------+
# |                POST EXAMPLE CLEANUP             |
# +-------------------------------------------------+

db.close()
