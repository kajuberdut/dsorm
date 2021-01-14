"""
D.S.O: Darned Simple ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""
import abc
import dataclasses
import functools
import sqlite3
import typing as t
from collections import defaultdict
from collections.abc import Iterable

from enum import Enum


# SECTION 1: SQL Command Clause Order Enumerations
class CreateTableType(Enum):
    CREATE = 1
    COLUMN = 2
    CONSTRAINT = 3


class SelectType(Enum):
    CTE = 1
    SELECT = 2
    FROM = 4
    JOIN = 6
    WHERE = 7
    GROUP = 9
    HAVING = 10
    ORDER = 11
    LIMIT = 12
    OFFSET = 13


class DeleteType(Enum):
    FROM = 1
    WHERE = 2


class InsertType(Enum):
    CTE = 1
    INSERT = 2
    COLUMNS = 3
    FROM = 4
    VALUES = 5


class UpdateType(Enum):
    CTE = 1
    UPDATE = 2
    SET = 3
    WHERE = 4


# SECTION 2: Custom Types, abc classes and base classes
@dataclasses.dataclass
class DSOObject(abc.ABC):
    @abc.abstractmethod
    def sql(self) -> str:
        ...  # pragma: no cover


class RegisteredObject(DSOObject):
    """ Registered Objects are automatically registered in the information schema of their database."""

    db: "Database" = None

    def __post_init__(self):
        if self.db is None:
            self.db = Database
        self.db.information_schema[type(self).__name__][self.name] = self


SQLFragment = t.Union[DSOObject, str]


# SECTION 3: Utility functions
nlta = "\n\tAND "  #  New Line, Tab, "AND"


def name(o: SQLFragment, qualify=False) -> str:
    if isinstance(o, DSOObject):
        if qualify:
            return o.identifier
        else:
            return o.name
    else:
        return o


qname = functools.partial(name, qualify=True)


def sql(o: SQLFragment) -> str:
    if isinstance(o, DSOObject):
        return o.sql()
    else:
        return o


def joinmap(
    o: t.Union[Iterable, SQLFragment], f: t.Callable = name, seperator: str = ", "
) -> str:
    """ Returns a comma seperated list of f(i) for i in o. """
    if isinstance(o, Iterable) and not isinstance(o, str):
        return seperator.join(map(f, o))
    else:
        return f(o)


def where_sql(where: t.Dict):
    if not where:
        return
    return (
        f"""WHERE {joinmap(where.keys(), f=lambda x: f"{x} = :{x}", seperator=nlta)}"""
    )


def init_db(db_path: str = None):
    """ Create basic db objects. """
    sql_set = list()
    [
        [sql_set.append(o) for o in Database.information_schema[t].values()]
        for t in ["Pragma", "Table"]
    ]
    with Cursor(db_path=db_path) as cur:
        seperator = ";\n\n"
        cur._cursor.executescript(joinmap(sql_set, sql, seperator=seperator))


# SECTION 4: SQL Component Classes
@dataclasses.dataclass
class Statement:
    """An object representing a sql statement and optional values."""

    statement_type: Enum
    components: t.Dict[Enum, str] = dataclasses.field(default_factory=dict)
    values: t.Dict = None
    _db: "Database" = None

    @property
    def sql(self) -> str:
        return "\n".join(
            [
                sql(self.components[clause])
                for clause in self.statement_type
                if clause in self.components
            ]
        )


@dataclasses.dataclass
class Pragma(RegisteredObject):
    pragma: t.Dict
    name: str = "MAIN"

    def sql(self):
        return ";\n".join([f"PRAGMA {k}={v}" for k, v in self.pragma.items()])


@dataclasses.dataclass
class Column(RegisteredObject):
    name: str
    sqltype: str = ""
    unique: bool = False
    nullable: bool = True
    pkey: bool = False
    _table: "Table" = None

    def sql(self):
        blocks = [self.name, self.sqltype]
        if not self.nullable:
            blocks.append("NOT NULL")
        if self.unique:
            blocks.append("UNIQUE")
        if self.pkey:
            blocks.append("PRIMARY KEY")
        return " ".join(blocks)

    @property
    def table(self) -> "Table":
        return self._table

    @table.setter
    def table(self, table: "Table") -> None:
        self._table = table

    @property
    def identifier(self):
        if self.table:
            return self.table.identifier + "." + self.name
        else:
            return self.name

    def __repr__(self):
        return self.identifier


@dataclasses.dataclass
class ForeignKey(RegisteredObject):
    column: t.Union[t.List, SQLFragment]
    reference_table: "Table"
    reference_column: t.List[SQLFragment]

    @property
    def name(self) -> str:
        return f"Fkey on {joinmap(self.column)}"

    def sql(self):
        return f"FOREIGN KEY ({joinmap(self.column)}) REFERENCES {name(self.reference_table)}({joinmap(self.reference_column)})"

    def __repr__(self):
        return f"FKEY {name(self.reference_table)}({joinmap(self.reference_column)})"


@dataclasses.dataclass
class Table(RegisteredObject):
    column: t.List
    name: str = None
    constraints: t.List = dataclasses.field(default_factory=list)
    schema: str = None

    def __post_init__(self):
        for c in self.column:
            c.table = self
        super().__post_init__()

    def sql(self):
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n {joinmap([*self.column, *self.constraints], sql)})"

    def pkey(self) -> t.List[DSOObject]:
        return [c for c in self.column if c.pkey]

    def fkey(self, on_column: SQLFragment = None) -> ForeignKey:
        primary = self.pkey()
        if on_column is None:
            on_column = primary
        return ForeignKey(
            column=on_column, reference_table=self, reference_column=primary
        )

    @property
    def identifier(self):
        if self.schema:
            return self.schema + "." + self.name
        else:
            return self.name

    def __repr__(self):
        return f"{self.identifier}({joinmap(self.column)})"

    def select(self, where: t.Dict = None, columns: t.List = None) -> Statement:
        return (
            f"""SELECT {joinmap(columns if columns else self.column, qname)} \n FROM {self.name} \n {where_sql(where)}""",
            where,
        )

    def insert(self, data: t.Dict, replace: bool = False):
        k = data.keys()
        return (
            f"""{"REPLACE" if replace else "INSERT"} INTO {self.name} ({joinmap(k)}) VALUES({joinmap(k, lambda x: f":{x}")});""",
            data,
        )

    def delete(self, where: t.Dict) -> None:
        return (
            f"""DELETE FROM {self.name} \n {where_sql(where)}""",
            where,
        )


# SECTION 5: Database
class Database:

    connection_pool: t.Dict[str, sqlite3.Connection] = dict()
    default_db: str = None
    information_schema: t.Dict = defaultdict(dict)

    @classmethod
    def table(self, o: SQLFragment) -> "DSOObject":
        if isinstance(o, Table):
            return o
        else:
            return self.information_schema["Table"][o]

    def __init__(self, db_path: str = None):
        if db_path:
            self.db_path = db_path
        elif self.default_db is None:
            raise ValueError("No default set, a db_path must be provided.")
        else:
            self.db_path = self.default_db

        if self.c is None:
            self.c = sqlite3.connect(self.db_path)
            self.c.row_factory = self.dict_factory

    def dict_factory(
        self, cursor: sqlite3.Cursor, row: sqlite3.Row
    ) -> t.Dict[t.Any, t.Any]:  # pragma: no cover
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    @property
    def c(self) -> t.Optional[sqlite3.Connection]:
        return self.connection_pool.get(self.db_path)

    @c.setter
    def c(self, connection: sqlite3.Connection) -> None:
        self.connection_pool[self.db_path] = connection

    def close(self):
        self.c.close()
        del self.connection_pool[self.db_path]

    def query(
        self, table: t.Union["Table", str], where: t.Dict = None, columns: t.List = None
    ) -> t.List:
        sql, values = self.table(table).select(where=where, columns=columns)
        with Cursor(_db=self) as cur:
            result = cur.execute(sql, values)
        return result

    def create(self, table: t.Union["Table", str], data: t.Dict) -> None:
        sql, data = self.table(table).insert(data=data)
        with Cursor(_db=self) as cur:
            cur.execute(sql, data)

    def delete(self, table: t.Union["Table", str], where: t.Dict) -> None:
        sql, values = self.table(table).delete(where=where)
        with Cursor(_db=self) as cur:
            cur.execute(sql, values)


class Cursor:
    """ A convenience class that wraps SQLite3.Cursor connected to a dso.Database instance. """

    def __init__(self, db_path=None, _db: Database = None, auto_commit=True):
        if _db:
            self._db = _db
        else:
            self._db = Database(db_path=db_path)
        self.auto_commit = auto_commit

    def __enter__(self) -> "Cursor":
        self._cursor = self._db.c.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.auto_commit:
            self._db.c.commit()
        self._cursor.close()

    def commit(self):
        self._db.c.commit()

    def execute(
        self,
        command: str,
        parameters: t.Union[t.Tuple, t.Dict] = None,
        commit: bool = True,
    ):
        """ Execute a sql command with optional parameters """
        if parameters:
            self._cursor.execute(command, parameters)
        else:
            self._cursor.execute(command)
        if commit:
            self.commit()
        return self._cursor.fetchall()
