"""
This longer example will demo building up complex where clauses.
It also includes an additional example of custom type handling
    as well as showing bulk inserts of records using random values.
"""

from enum import IntEnum
from random import randint

from dsorm import Column, Database, TypeHandler, Table, TypeMaster, Where


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
        return str(int(value))


TypeMaster.register(GenderHandler)

person = Table(
    name="Person",
    column=[
        Column(name="person_id", python_type=int, pkey=True),
        Column(name="gender", python_type=Gender),
        Column(name="age", python_type=int),
    ],
)

Database.default_db = ":memory:"
db = Database()
db.init_db()

data = [{"gender": Gender(randint(1, 2)), "age": randint(1, 99)} for i in range(10)]

print(f"Example data: {data[0]}")
# Example data: {'gender': <Gender.FEMALE: 1>, 'age': 75}

db.insert(table="Person", data=data)

# Let's use this as an example where we have different criteria groups
# Male and 65 or older
m = Where(where={"gender": Gender.MALE, "age": Where.greater_than_or_equal(target=65)})
# Female and 67 or older
f = Where(
    where={"gender": Gender.FEMALE, "age": Where.greater_than_or_equal(target=67)}
)

complex_where = Where({"": f, "OR": m})  # Note that the first clause has an empty key

# Example of a complex where clause:
# WHERE ( gender = 1
#         AND [age] >= 67
#       )
# OR    ( gender = 2
#         AND [age] >= 65
#       )

print(f"Example of a complex where clause: {complex_where.sql()}")

results = db.query("Person", where=complex_where)

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
