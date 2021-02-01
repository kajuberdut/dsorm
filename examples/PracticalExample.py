"""
Leverage a database to store large texts.
See the other examples for more details.
"""

import dataclasses
import typing as t
from hashlib import md5

from dsorm import Column, Database, Table


# If the function set as a column default has a named parameter "data"
# It will be passed the row data on insert
def set_hash(data: t.Dict) -> str:
    return md5(data["text"].encode("utf-8")).hexdigest()


book_table = Table(
    name="book",
    column=[
        Column("hash", unique=True, pkey=True, default=set_hash),
        Column("name", str, unique=True),
        Column("text", str),
    ],
)


@dataclasses.dataclass
class Book:
    name: str
    text: str
    hash: str = None
    auto_save: dataclasses.InitVar = False

    def __post_init__(self, auto_save):
        if auto_save:
            self.save()

    @classmethod
    def book_from_name(cls, name):
        return cls(**book_table.select(where={"name": name}).execute()[0])

    def save(self):
        book_table.insert(data=dataclasses.asdict(self), replace=True).execute()

    def __getitem__(self, key: slice) -> str:
        if not isinstance(key, slice):
            raise ValueError("Book.__getitem__ expects a slice")
        c = (
            "text"
            if key.start is None
            else (
                f"substr(text, {key.start})"
                if key.stop is None
                else f"substr(text, {key.start}, {key.stop - key.start})"
            )
        )
        return book_table.select(column=[f"{c} AS text"]).execute()[0]["text"]

    def __repr__(self):
        return f"Book(name={self.name})"


if __name__ == "__main__":
    # See Advanced Configuration for how to defer these
    Database(db_path=":memory:", is_default=True).init_db()

    name = "The Worst Book in the World"
    Book(name=name, text=(name * 10000), auto_save=True)

    # Meanwhile back at the bat cave
    b = Book.book_from_name(name)

    print(b.hash)

    print(b[9000:9010])
