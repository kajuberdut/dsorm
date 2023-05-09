import sqlite3

from dsorm import DBClass, dblite

# +-------------------------------------------------+
# |                PRE EXAMPLE SETUP                |
# +-------------------------------------------------+


# Table must exist before the @sqlite_dbclass is created.
db = sqlite3.connect(":memory:")
db.execute(
    """CREATE TABLE user (id INTEGER PRIMARY KEY NOT NULL,
                          first_name TEXT NOT NULL,
                          last_name TEXT NOT NULL,
                          email TEXT UNIQUE NOT NULL)"""
)


# +-------------------------------------------------+
# |                EXAMPLE STARTS HERE              |
# +-------------------------------------------------+


# The db class decorator takes at minimum an instance of databases.Database
@dblite(db=db)
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

print(f"new_user isinstant of DBClass: {isinstance(new_user, DBClass)}")

# load the same user from the db
loaded_user = User.load(new_user.id)

print(f"{loaded_user.id=}")
print(f"full name: {loaded_user.first_name} {loaded_user.last_name}")
print(f"{loaded_user.email=}")


# +-------------------------------------------------+
# |                POST EXAMPLE CLEANUP             |
# +-------------------------------------------------+

db.close()
