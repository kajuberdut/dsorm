"""
D.S.O: Darned Simple ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""
import dataclasses
import functools
import sqlite3
import typing as t
from collections import defaultdict
from collections.abc import Iterable

from enum import Enum


# SECTION 2: Custom Types, abc classes and base classes
class DSObject:
    ...


class Special(Enum):
    Null = 1
    NotNull = 2
    TBD = 3


class RegisteredObject(DSObject):
    """ Registered Objects are automatically registered in the information schema of their database."""

    db: "Database" = None

    def __post_init__(self):
        self.register()

    def register(self):
        if self.db is None:
            self.db = Database
        self.db.information_schema[type(self).__name__][self.name] = self


SQLFragment = t.Union[DSObject, str, int, float]
Fragments = t.Union[Iterable, SQLFragment]
ComparisonOperator = t.Literal["=", ">", "<", "!=", "<>", ">=", "<="]

# SECTION 3: Utility functions
LINE_AND_SEPERATOR = "\n\tAND "
COMMAND_SEPERATOR = ";\n\n"


def ds_name(o: SQLFragment, qualify=False) -> str:
    if isinstance(o, DSObject):
        if qualify:
            return o.identifier
        else:
            return o.name
    else:
        return o


ds_qname = functools.partial(ds_name, qualify=True)


def ds_sql(o: SQLFragment) -> str:
    if isinstance(o, DSObject):
        return o.sql()
    else:
        return o


def joinmap(o: Fragments, f: t.Callable = ds_name, seperator: str = ", ") -> str:
    """ Returns a comma seperated list of f(i) for i in o. """
    if isinstance(o, Iterable) and not isinstance(o, str):
        try:
            return seperator.join(map(f, o))
        except TypeError:
            return seperator.join([str(o) for o in map(f, o)])
    else:
        return f(o)


def ds_where(where: t.Union["Where", t.Dict]) -> "Where":
    if isinstance(where, Where):
        return where
    else:
        return Where(where)


def ds_quote(o: t.Any) -> t.Union[str, int, float]:
    if isinstance(o, DSObject):
        return o.cast()
    else:
        cast = {str: lambda x: f"'{x}'", int: str, float: str}[type(o)]
        return cast(o)


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
class SQLType:
    class PickleList(DSObject):
        def sql(self):
            return ""

    class PickledDict(DSObject):
        def sql(self):
            return ""


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
                ds_sql(self.components[clause])
                for clause in self.statement_type
                if clause in self.components
            ]
        )

    class StatementOrder(Enum):
        CTE = 1
        UPDATE = 2
        SET = 3
        INSERT = 4
        INSERT_COLUMNS = 5
        SELECT_COLUMNS = 6
        FROM = 7
        JOIN = 8
        VALUES = 9
        WHERE = 10
        GROUP = 11
        HAVING = 12
        ORDER = 13
        LIMIT = 14
        OFFSET = 15


@dataclasses.dataclass
class Where(DSObject):
    where: t.Dict

    def sql(self):
        if not self.where:
            return ""
        clause_list = list()
        for k, v in self.where.items():
            if isinstance(v, (str, int, float)):
                v = self.get_comparison(column=k, target=v)
            if hasattr(v, "column") and v.column == Special.TBD:
                v.column = Column(name=k)

            clause_list.append(v)
        return f"""WHERE {joinmap(clause_list, ds_sql, seperator=LINE_AND_SEPERATOR)}"""

    @dataclasses.dataclass
    class Comparison(DSObject):
        column: SQLFragment
        target: SQLFragment
        operator: ComparisonOperator

        def sql(self):
            if self.column == Special.TBD:
                raise TypeError("Column argument is required")
            return f"{ds_qname(self.column)} {self.operator} {ds_quote(self.target)}"

    @classmethod
    def get_comparison(
        cls,
        column: SQLFragment = Special.TBD,
        target: SQLFragment = None,
        operator: ComparisonOperator = "=",
    ) -> "Where.Comparison":
        if target is None:
            raise TypeError("target argument is required")
        return cls.Comparison(column=column, target=target, operator=operator)

    equal = eq = functools.partialmethod(get_comparison, operator="=")
    not_equal = ne = functools.partialmethod(get_comparison, operator="!=")
    greater_than = gt = functools.partialmethod(get_comparison, operator=">")
    less_than = lt = functools.partialmethod(get_comparison, operator="<")

    @dataclasses.dataclass
    class In(DSObject):
        column: SQLFragment
        target: Fragments
        invert: bool = False

        def sql(self):
            return f"""{ds_qname(self.column)} {"NOT" if self.invert else ""} IN ({joinmap(self.target, ds_quote)})"""

    @classmethod
    def is_in(
        cls,
        column: SQLFragment = Special.TBD,
        target: Fragments = None,
        invert: bool = False,
    ) -> "Where.In":
        if target is None:
            raise TypeError("target argument is required")
        return cls.In(column=column, target=target, invert=invert)

    not_in = functools.partialmethod(is_in, invert=True)


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
            return f"[{self.table.name}].[{self.name}]"
        else:
            return f"[{self.name}]"

    def __repr__(self):
        return self.identifier

    def cast(self):
        return ".".join(f"[{s.strip('][')}]" for s in self.identifier.split("."))


@dataclasses.dataclass
class ForeignKey(RegisteredObject):
    column: t.Union[t.List, SQLFragment]
    reference_table: "Table"
    reference_column: t.List[SQLFragment]

    @property
    def name(self) -> str:
        return f"Fkey on {joinmap(self.column)}"

    def sql(self):
        return f"FOREIGN KEY ({joinmap(self.column)}) REFERENCES {ds_name(self.reference_table)}({joinmap(self.reference_column)})"

    def __repr__(self):
        return f"FKEY {ds_name(self.reference_table)}({joinmap(self.reference_column)})"


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
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n {joinmap([*self.column, *self.constraints], ds_sql)})"

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
        return f"""SELECT {joinmap(columns if columns else self.column, ds_qname)} \n FROM {self.name} \n {ds_sql(ds_where(where))}"""

    def insert(self, data: t.Dict, replace: bool = False):
        k = data.keys()
        return (
            f"""{"REPLACE" if replace else "INSERT"} INTO {self.name} ({joinmap(k)}) VALUES({joinmap(k, lambda x: f":{x}")});""",
            data,
        )

    def delete(self, where: t.Dict) -> None:
        return (
            f"""DELETE FROM {self.name} \n {ds_where(where).sql()}""",
            where,
        )

    class CreateTableType(Enum):
        CREATE = 1
        COLUMN = 2
        CONSTRAINT = 3


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
        with Cursor(_db=self) as cur:
            sql = self.table(table).select(where=where, columns=columns)
            result = cur.execute(sql)
        return result

    def create(self, table: t.Union["Table", str], data: t.Dict, replace=False) -> None:
        with Cursor(_db=self) as cur:
            cur.execute(*self.table(table).insert(data=data, replace=replace))

    def delete(self, table: t.Union["Table", str], where: t.Dict) -> None:
        with Cursor(_db=self) as cur:
            cur.execute(*self.table(table).delete(where=where))

    def init_db(self):
        """ Create basic db objects. """
        sql_set = list()
        [
            [sql_set.append(o) for o in self.information_schema[t].values()]
            for t in ["Pragma", "Table"]
        ]
        script = joinmap(sql_set, ds_sql, seperator=COMMAND_SEPERATOR)
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
