"""
Leverage a database to store large texts.
See the other examples for more details.
"""

import dataclasses
import typing as t
from hashlib import md5

from dsorm import Column, Database, Table, post_connect, pre_connect


# See Advanced Configuration:
@pre_connect()
def db_setup(db):
    db.default_db = ":memory:"


@post_connect()
def build(db):
    db.init_db()


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
        return cls(**Database().query("book", where={"name": name})[0])

    def save(self):
        Database().insert("book", data=dataclasses.asdict(self), replace=True)

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
        return Database().query("book", columns=[f"{c} AS text"])[0]["text"]

    def __repr__(self):
        return f"Book(name={self.name})"


if __name__ == "__main__":
    name = "The Worst Book in the World"
    Book(name=name, text=(name * 10000), auto_save=True)

    # Meanwhile back at the bat cave
    b = Book.book_from_name(name)

    print(b.hash)

    print(b[9000:9010])
