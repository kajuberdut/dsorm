import dataclasses
from enum import Enum

from dsorm import Comparison, Database, DataClassTable, make_table

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

stmt = person.insert(
    data=[
        {"first_name": "John", "last_name": "Doe", "team": Team.BLUE},
    ],
).execute()

Jane = Person(first_name="Jane", last_name="Doe", team=Team.RED).save()

person.delete(
    where={
        "id": person.select(
            where={"first_name": Comparison.like(target="J%n%")},
        ).execute()[0]["id"]
    }
).execute()
print(person.select(column=["id", "first_name"]).execute())
