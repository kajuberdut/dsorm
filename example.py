from dso import Column, Cursor, Database, ForeignKey, Pragma, Table, init_db

Database.default_db = ":memory:"

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
    # This will set our pragam and create our tables from above
    init_db()

    # Table objects have select, insert (can update)
    # #, and delete methods that simply return sql you can execute
    sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})

    # The above outputs sql like this:
    # INSERT INTO person ( first_name
    #                    , last_name
    #                    )
    # VALUES(:first_name, :last_name);

    with Cursor() as cur:
        cur.execute(sql, values)
        print(cur.execute(*Person.select()))
        # {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}

    # Even more convenient:
    # The db object can access any table and run the whole thing for you.
    db = Database()

    # Create inserts a record
    db.create(table="person", data={"first_name": "Jane", "last_name": "Doe"})

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
    # Finally delete the first record
    db.delete("person", where={"id": johns[0]["id"]})
