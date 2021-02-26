"""
This longer example will demo building up complex where clauses.
It also includes an additional example of custom type handling
    as well as showing bulk inserts of records using random values.
"""

from enum import IntEnum
from random import randint

from dsorm import Column, Database, Table, TypeHandler, Where


class Gender(IntEnum):
    FEMALE = 1
    MALE = 2


class GenderHandler(TypeHandler):
    python_type = Gender
    sql_type = "INTEGER"

    @staticmethod
    def s2p(value) -> Gender:
        return Gender(value)

    @staticmethod
    def p2s(value) -> int:
        return int(value)


GenderHandler.register()

person = Table(
    name="Person",
    column=[
        Column(name="person_id", python_type=int, pkey=True),
        Column(name="gender", python_type=Gender),
        Column(name="age", python_type=int),
    ],
)

Database(db_path=":memory:", is_default=True).init_db()

data = [{"gender": Gender(randint(1, 2)), "age": randint(1, 99)} for i in range(10)]

print(f"Example data: {data[0]}")
# Example data: {'gender': <Gender.FEMALE: 1>, 'age': 75}

person.insert(data=data).execute()

# We'll make a select statement from this table next
stmt = person.select()
# This will have an empty dict in it's .where attribute

# Any value in a where dictionary can contain another where clause
# The sub-where will be nested in () automatically but you will need to
#   set "OR", "AND" or "" in the key.

print(stmt.where)

# Let's use this as an example where we have different criteria groups
stmt.where[""] = Where(  # Empty key since we don't want to start with "AND" or "OR"
    where={"gender": Gender.MALE, "age": Where.greater_than_or_equal(target=65)}
)
# Since modern python dictionaries keep insert order we can just do these in order
stmt.where["OR"] = Where(
    where={"gender": Gender.FEMALE, "age": Where.greater_than_or_equal(target=67)}
)

# Example of a complex where clause:
# WHERE ( gender = 1
#         AND [age] >= 67
#       )
# OR    ( gender = 2
#         AND [age] >= 65
#       )

print(f"Example of a complex where clause: {stmt.where.sql()}")

results = stmt.execute()

print(f"All male records age 65+ and all female records 67+:\n  {results}")
# All male records age 65+ and all female records 67+:
# [
#     {'person_id': 1, 'gender': <Gender.FEMALE: 1>, 'age': 75
#     },
#     {'person_id': 2, 'gender': <Gender.MALE: 2>, 'age': 70
#     },
#     {'person_id': 6, 'gender': <Gender.FEMALE: 1>, 'age': 80
#     },
#     {'person_id': 7, 'gender': <Gender.FEMALE: 1>, 'age': 67
#     },
#     {'person_id': 8, 'gender': <Gender.MALE: 2>, 'age': 80
#     },
#     {'person_id': 10, 'gender': <Gender.MALE: 2>, 'age': 81
#     }
# ]
