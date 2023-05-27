import asyncio
import inspect
import sqlite3
import typing

if typing.TYPE_CHECKING:
    from databases import Database


def resolve(func, *args, **kwargs):
    if asyncio.get_event_loop() is not None:
        raise RuntimeError("An event loop is already present.")

    result = func(*args, **kwargs)
    if inspect.iscoroutine(result):
        return asyncio.run(result)
    else:
        return result


def scaffold_db_class(
    dbclass, db: typing.Union[sqlite3.Connection, "Database"], table_name: str | None
):
    dbclass._is_db_class = True

    if table_name is None:
        table_name = dbclass.__name__.lower()

    setattr(dbclass, "_table_name", table_name)
    setattr(dbclass, "db", db)

    signature = inspect.signature(dbclass.__init__)
    if "data" not in signature.parameters:
        # Add _data to init
        init = dbclass.__init__

        def new_init(self, *args, **kwargs):
            data = kwargs.pop("data", {})
            setattr(self, "_data", dict(data))
            init(self, *args, **kwargs)

        dbclass.__init__ = new_init


def add_property(cls, col_name: str):
    "Utility function to add getter/setters for a property that targets self._data."

    def getter(self):
        return getattr(self, "_data").get(col_name)

    def setter(self, value):
        getattr(self, "_data")[col_name] = value

    setattr(cls, col_name, property(getter, setter))
