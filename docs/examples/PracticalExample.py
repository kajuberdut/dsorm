"""
Leverage a database to store large texts.
See the other examples for more details.
"""

import dataclasses
import typing as t
from hashlib import md5

from dsorm import Database, DataClassTable, RawSQL


# If the function set as a column default has a named parameter "data"
# It will be passed the row data on insert
def set_hash(data: t.Optional[t.Dict] = None) -> t.Optional[str]:
    if data:
        return md5(data["text"].encode("utf-8")).hexdigest()


@dataclasses.dataclass
class Book(DataClassTable):
    name: t.Optional[str] = dataclasses.field(default=None, metadata={"unique": True})
    text: t.Optional[str] = None
    hash: t.Optional[str] = dataclasses.field(
        default_factory=set_hash,
        metadata={
            "column_name": "hash",
            "unique": True,
            "pkey": True,
            "exclude_data": True,
        },
    )

    @classmethod
    def book_from_name(cls, name):
        return cls(**cls.get_table().select(where={"name": name}).execute()[0])

    def __getitem__(self, key: slice) -> str:
        """An edge case where RawSQL is used to limit the amount
        of text being transmitted.
        """
        if not isinstance(key, slice):
            raise ValueError("Book.__getitem__ expects a slice")
        c = (
            "text"
            if key.start is None
            else (
                RawSQL(f"substr(text, {key.start}) AS text")
                if key.stop is None
                else RawSQL(
                    f"substr(text, {key.start}, {key.stop - key.start}) AS text"
                )
            )
        )
        return self.table.select(column=[c]).execute()[0]["text"]

    def __repr__(self):
        return f"Book(name={self.name})"


if __name__ == "__main__":
    db = Database.memory()

    name = "The Worst Book in the World"
    Book(db_path=":memory:", name=name, text=(name * 10000)).save()

    # Meanwhile back at the bat cave
    b = Book.book_from_name(name)

    print(b.hash)
    # c0f13299dc9a5d5820d384addb8ccd02

    print(b[9000:9010])
    # t Book in
