from dsorm import *  # clean for readme, you'll want to import objects by name.

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

Email = Table(
    name="email",
    column=[
        Column("id", sqltype="INTEGER", pkey=True),
        Column("email", sqltype="TEXT", nullable=False),
        Column("person_id", nullable=False),
        Person.fkey(on_column="person_id"),
    ],
)

if __name__ == "__main__":
    # Table objects have select, insert, and delete methods
    # Each returns sql and values you can pass to execute
    sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})
    with Cursor() as cur:
        cur.execute(sql, values)
        print(cur.execute(*Person.select()))
        # {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}

    # Even more convenient:
    # The db object can access any table and run the whole thing for you.
    db = Database()

    # Create inserts a record
    db.create(table="person", data={"first_name": "John", "last_name": "Doe"})

    # Query selects back a list of dicts matching the where clause
    johns = db.query(
        "person",
        where={"first_name": "John"},
        columns=[
            "id",
            "first_name || ' ' || last_name AS full_name",
        ],  # Note that the columns can be freehand sql
    )
    print(johns)
    # [{'id': 1, 'full_name': 'John Doe'}, {'id': 2, 'full_name': 'John Doe'}]

    db.delete("person", where={"id": johns[0]["id"]})
    print(db.query("person"))
    # [{'id': 2, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}]
