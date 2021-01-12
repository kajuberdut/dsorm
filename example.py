from dso import Column, Database, ForeignKey, Pragma, Table, init_db

Database.set_default_db(":memory:")

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
        ForeignKey(column="person_id", reference_table=Person, reference_column="id"),
    ],
)

if __name__ == "__main__":
    # This will set our pragam and create our tables from above
    init_db()

    with Database() as db:

        # Table objects have select, insert (can update), and delete methods that simply return sql you can execute
        sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})

        print(sql)
        # INSERT INTO person ( first_name
        #                    , last_name
        #                    )
        # VALUES(:first_name, :last_name);

        db.execute(sql, values)

        print(db.execute(*Person.select()).fetchone())
        # {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}

        # Even more convenient, the db object can access any table and run the whole thing for you.
        # Create inserts a record
        db.create(table="person", data={"first_name": "John", "last_name": "Doe"})
        # query selects back a list of records matching the where clause
        johns = db.query(
            "person",
            where={"first_name": "John"},
            columns=[
                "id",
                "first_name || ' ' || last_name AS full_name",
            ],  # Note that the columns can be freehand sql
        )
        print(johns)
        # Finally delete
        db.delete("person", where={"id": johns[0]["id"]})
