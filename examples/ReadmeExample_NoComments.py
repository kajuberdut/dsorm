import dataclasses
from enum import Enum

from dsorm import Database, DataClassTable, make_table

db = Database.memory()


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


person.insert(
    data=[
        {"first_name": "Jane", "last_name": "Doe", "team": Team.RED},
        {"first_name": "John", "last_name": "Doe", "team": Team.BLUE},
    ],
).execute()

person.delete(where={"first_name": "John"}).execute()
print(person.select(column=["first_name"]).execute())
