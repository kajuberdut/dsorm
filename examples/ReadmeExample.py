import dataclasses
from enum import Enum

from dsorm import Comparison, Database, DataClassTable, make_table

# the .memory() constructor is equivilent to Database(db_path=":memory:", is_default=True)
db = Database.memory()


# Leverage enums for efficient small lookup tables
@make_table
class Team(Enum):
    UNASSIGNED = 0
    RED = 1
    BLUE = 2


@make_table
@dataclasses.dataclass
class Person(DataClassTable):
    first_name: str = None
    last_name: str = None
    team: Team = Team.UNASSIGNED


person = db.table("Person")

print(person.sql())
# CREATE TABLE IF NOT EXISTS person (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT);


# Tables have insert, select, and delete methods which return subclasses of dsorm.Statement
stmt = person.insert(
    data=[
        {"first_name": "Jane", "last_name": "Doe", "team": Team.RED},
        {"first_name": "John", "last_name": "Doe", "team": Team.BLUE},
    ],
)

# Statements can be examined with .sql method
print(stmt.sql())
# INSERT INTO [Person] (first_name, last_name, team) VALUES ('Jane', 'Doe', 1), ('John', 'Doe', 2)

# or executed with .execute()
stmt.execute()

# Select returns a list of dicts of rows matching the where
doe_family = person.select(
    where={"first_name": Comparison.like(target="J%n%")},
).execute()

print(doe_family)
# [
#     {'id': 1, 'first_name': 'Jane', 'last_name': 'Doe', 'team': <Team.RED: 1>
#     },
#     {'id': 2, 'first_name': 'John', 'last_name': 'Doe', 'team': <Team.BLUE: 2>
#     }
# ]

# And Delete
person.delete(where={"id": doe_family[0]["id"]}).execute()
print(person.select(column=["first_name"]).execute())
# [{'first_name': 'John'}]
