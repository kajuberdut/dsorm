from dsorm import *  # Don't do this in real code: https://www.python.org/dev/peps/pep-0008/#imports

# The pre_connect wrapper let's you set a function that will be called before the first connection
@pre_connect()
def db_setup(db):
    db.default_db = ":memory:"


# The post_connect wrapper is called once after the first connection is made
@post_connect()
def build(db):
    # This will set our pragam and create our tables.
    # We'll create these bellow and they will be instantiated at the first connection
    db.init_db()


# Let's setup foreign key enforcement which is off by default
conf = Pragma(
    pragma={
        "foreign_keys": 1,
        "temp_store": 2,
    }
)

Person = Table(
    name="person",
    column=[
        Column("id", sqltype="INTEGER", pkey=True),
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
        Column("screen_name", unique=True),
    ],
)

# Table objects have select, insert, and delete methods
# Each returns sql and values you can use with execute
sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})
with Cursor() as cur:
    cur.execute(sql, values)
    # Or with unpacking (*)
    print(cur.execute(*Person.select()))

# Even more convenient:
# Database instances can access any table with Create, Query, or Delete.
db = Database()

# Inserts a record
db.create(table="person", data={"first_name": "John", "last_name": "Doe"})

# Select a list of rows matching the where
johns = db.query(
    "person",
    where={"first_name": "John"},
    columns=[
        "id",
        "first_name || ' ' || last_name AS full_name", # Note that the columns can be sql
    ],  
)
print(johns)

db.delete("person", where={"id": johns[0]["id"]})
print([r["id"] for r in db.query("person")])
