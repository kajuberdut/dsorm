import dataclasses

from dsorm import tablify


def bob():
    return "bob"


@dataclasses.dataclass
class Thing:
    stuff: str = dataclasses.field(
        default_factory=bob, metadata={"column_name": "bob", "unique": True}
    )
    value: int = dataclasses.field(default=1)


thing_table = tablify(Thing)
print(thing_table.sql())
print(thing_table["bob"].default)
