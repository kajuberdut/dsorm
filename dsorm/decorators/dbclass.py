import asyncio
import typing

from databases import Database
from dsorm.decorators.typing import DBClass
from dsorm.utility import add_property, basic_db_class


# dbclass methods
async def save(self) -> DBClass:
    """Saves the _data values by updating if _data.id is an integer
    or inserting otherwise.
    """

    column_names = [key for key in self._data.keys() if key != "id"]
    value_names = [f":{column_name}" for column_name in column_names]

    try:
        id = int(self._data.get("id"))
    except TypeError:
        id = False

    if id:
        # When ID is an integer > 0 we update
        update_pairs = [
            f"{column} = {value}" for column, value in zip(column_names, value_names)
        ]
        query = f"""
        UPDATE {self.__class__._table_name}
        SET ({",".join(update_pairs)})
        WHERE id = :id
        """
        await self.__class__._db.execute(query=query, values=self._data)
    else:
        # Insert
        query = f"""
        INSERT INTO {self.__class__._table_name} ({",".join(column_names)})
        VALUES({",".join(value_names)})
        """
        inserted_key = await self.__class__._db.execute(query=query, values=self._data)
        self.id = inserted_key
    return self


@classmethod
async def load(cls, pkey_value, **kwargs) -> DBClass:
    "Returns an instances of cls with _data set from the db retrieved by primary key."
    query = f"SELECT * FROM {cls._table_name} WHERE id = :id"
    result = await cls._db.fetch_one(query=query, values={"id": pkey_value})
    return cls(data=result._mapping, **kwargs)


@classmethod
async def select(
    cls, where_clause: str, values: dict, **kwargs
) -> typing.List[DBClass]:
    "Returns a list of cls matching where_clause."
    query = f"SELECT * FROM {cls._table_name} WHERE {where_clause}"
    result = await cls._db.fetch_all(query=query, values=values)
    return [cls(data=row._mapping, **kwargs) for row in result]


async def delete(cls, pkey_value: int | None = None) -> None:
    "Deletes the corresponding row from the db based on primary key."
    if pkey_value is None:
        pkey_value = cls.id
    query = f"DELETE FROM {cls._table_name} WHERE id = :id"
    await cls._db.execute(query=query, values={"id": pkey_value})


def dbclass(db: Database, table_name: str | None = None):
    """
    A decorator which enables basic database operations for the decorated class.
    Takes an instance of databases.Database.
    Optionally a table name can be provided, if not provided the class
        must share a name, ignoring case, with it's table.
    A data parameter is added to __init__ if not present and a _data property
        is created which stores a mapping of column values.
    Getters and setters are added for all columns so they can be retrieved / set with dot notation.
    Save, Load, Select, and Delete methods are added.
    """

    def class_decorator(dbclass):
        nonlocal table_name

        basic_db_class(dbclass, db, table_name)

        results = asyncio.run(db.fetch_all(f"PRAGMA table_info({dbclass._table_name})"))

        columns = [row._mapping["name"] for row in results]

        [add_property(dbclass, col_name=col) for col in ["id", *columns]]

        setattr(dbclass, "save", save)
        setattr(dbclass, "load", load)
        setattr(dbclass, "select", select)
        setattr(dbclass, "delete", delete)
        return dbclass

    return class_decorator
