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
class DSObject(abc.ABC):
    @abc.abstractmethod
    def sql(self) -> str:
        ...  # pragma: no cover


class RegisteredObject(DSObject):
    """ Registered Objects are automatically registered in the information schema of their database."""

    db: "Database" = None

    def __post_init__(self):
        if self.db is None:
            self.db = Database
        self.db.information_schema[type(self).__name__][self.name] = self


SQLFragment = t.Union[DSObject, str]


# SECTION 3: Utility functions
nlta = "\n\tAND "  #  New Line, Tab, "AND"


def name(o: SQLFragment, qualify=False) -> str:
    if isinstance(o, DSObject):
        if qualify:
            return o.identifier
        else:
            return o.name
    else:
        return o


qname = functools.partial(name, qualify=True)


def sql(o: SQLFragment) -> str:
    if isinstance(o, DSObject):
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
        return ""
    return (
        f"""WHERE {joinmap(where.keys(), f=lambda x: f"{x} = :{x}", seperator=nlta)}"""
    )


def do_nothing(*args, **kwargs):
    pass


def pre_connect(run_once=True):
    def pre_wrapper(func):
        @functools.wraps(func)
        def pre(*args):
            func(args[0])
            if run_once:
                Database.pre_connect_hook = do_nothing

        Database.pre_connect_hook = pre

    return pre_wrapper


def post_connect(run_once=True):
    def post_wrapper(func):
        @functools.wraps(func)
        def post(*args):
            func(args[0])
            if run_once:
                Database.post_connect_hook = do_nothing

        Database.post_connect_hook = post

    return post_wrapper


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
        if self.table and self.table.name:
            return self.table.name + "." + self.name
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
    schema: str = "Main"

    def __post_init__(self):
        for c in self.column:
            c.table = self
        super().__post_init__()

    def sql(self):
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n {joinmap([*self.column, *self.constraints], sql)})"

    def pkey(self) -> t.List[DSObject]:
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
    _default_db: str = None
    information_schema: t.Dict = defaultdict(dict)
    pre_connect_hook: t.Callable = do_nothing
    post_connect_hook: t.Callable = do_nothing

    @classmethod
    def table(self, o: SQLFragment) -> "DSObject":
        if isinstance(o, Table):
            return o
        else:
            return self.information_schema["Table"][o]

    @property
    def default_db(self):
        return self.__class__._default_db

    @default_db.setter
    def default_db(self, new):
        self.__class__._default_db = new

    def __init__(self, db_path: str = None):
        self.db_path = db_path
        if self.db_path is not None:
            self._c = self.connection_pool.get(self.db_path)
        else:
            self._c = None

    def dict_factory(
        self, cursor: sqlite3.Cursor, row: sqlite3.Row
    ) -> t.Dict[t.Any, t.Any]:  # pragma: no cover
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    def connect(self):
        self.pre_connect_hook()
        if self.db_path is None and self.default_db is not None:
            self.db_path = self.default_db
        self._c = self.connection_pool.get(self.db_path)
        if self._c is None:
            self.connection_pool[self.db_path] = sqlite3.connect(self.db_path)
            self._c = self.connection_pool[self.db_path]
            self._c.row_factory = self.dict_factory
        self.post_connect_hook()

    @property
    def c(self) -> sqlite3.Connection:
        if self._c is None:
            self.connect()
        return self._c

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

    def create(self, table: t.Union["Table", str], data: t.Dict, replace=False) -> None:
        sql, data = self.table(table).insert(data=data, replace=replace)
        with Cursor(_db=self) as cur:
            cur.execute(sql, data)

    def delete(self, table: t.Union["Table", str], where: t.Dict) -> None:
        sql, values = self.table(table).delete(where=where)
        with Cursor(_db=self) as cur:
            cur.execute(sql, values)

    def init_db(self):
        """ Create basic db objects. """
        sql_set = list()
        [
            [sql_set.append(o) for o in self.information_schema[t].values()]
            for t in ["Pragma", "Table"]
        ]
        seperator = ";\n\n"
        script = joinmap(sql_set, sql, seperator=seperator)
        with Cursor(_db=self) as cur:
            cur._cursor.executescript(script)


class Cursor:
    """ A convenience class that wraps SQLite3.Cursor connected to a dsorm.Database instance. """

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