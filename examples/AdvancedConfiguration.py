"""
No configuration beyond a db_path is needed to use dsorm as a connection manager.
However, a default db simplifies most use.
Setting a default in your __init__.py file will quickly lead to difficulty.
If you want a seperate test database, or want to get database
name from a config file.
It's also not a good practice to create the database file when a user simplyq
imports your code.

For these situations two decorators are provided that allow you to push the
    operations of setting your config and instantiating your database to just
    before and just after the first connection.

You have seen in previous examples the Database().initialize() method that creates
    all tables. In this example you will also see the Pragma object which
    allows you to set SQLite3 runtime configurations called pragma.
"""

from dsorm import Database, Pragma, pre_connect


# The pre_connect wrapper let's you set a function that will be called before the first connection
@pre_connect()
def db_setup(db):
    # This would be a good spot to check if you are running in a Dev/Prod/Test context
    # Or you could have already loaded a config for your run context in which case
    # you would simply pull in the database name here
    db.default_db = ":memory:"


# This pragma object will automatically be picked up by initialize and run.
# Note that because the post_connect wrapper sets up the 'build' function
#   to run just after first connect we can create pragma after build.
conf = Pragma.from_dict(
    {
        "foreign_keys": 1,  # Foreign key enforcement is off by default in SQLite
        "temp_store": 2,  # Don't copy this setting unless you know what it does
    }
)


db = Database()
# It's unusual to directly call connect
db.connect()  # here we use this to trigger the hooks from above

print(f"Default db is now {db.default_db}")

print(db.execute("PRAGMA foreign_keys"))
print(db.execute("PRAGMA temp_store"))
