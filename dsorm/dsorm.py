"""
dsORM: Data Structure ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""

import dataclasses
import functools
import inspect
import os
import pickle
import re
import sqlite3
import typing as t
from abc import ABCMeta, abstractmethod
from base64 import b32encode
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from enum import Enum, EnumMeta
from inspect import getattr_static, signature
from sqlite3.dbapi2 import OperationalError, ProgrammingError


# SECTION 1: Types / Literals
WhereLike = t.Union["Where", t.Dict]
LINE: str = "\n"
TAB = "\t"
ID_COLUMN = {"python_type": int, "pkey": True}
KEYWORDS = {
    "OPENPAREN": "(",
    "CLOSEPAREN": ")",
    "FOREIGNKEY": "FOREIGN KEY",
    "REFERENCES": "REFERENCES",
    "UNIQUER": "UNIQUE",
    "TERMINATOR": ";\n",
}


# SECTION 2: Database
class Database:

    connection_pool: t.Dict[str, sqlite3.Connection] = dict()
    _default_db: t.Optional[str] = None
    information_schema: t.Dict = defaultdict(dict)
    pre_connect_hook: t.Callable = lambda x: x
    post_connect_hook: t.Callable = lambda x: x

    @classmethod
    def memory(cls):
        return cls(db_path=":memory:", is_default=True)

    @property
    def default_db(self):
        return self.__class__._default_db

    @default_db.setter
    def default_db(self, new):
        self.__class__._default_db = new

    def __init__(self, db_path: str = None, is_default=False):
        self.db_path = db_path
        if self.db_path is not None:
            self._c = self.connection_pool.get(self.db_path)
        else:
            self._c = None
        if is_default and db_path is not None:
            self.__class__._default_db = db_path

    def dict_factory(
        self, cursor: sqlite3.Cursor, row: sqlite3.Row
    ) -> t.Dict[t.Any, t.Any]:  # pragma: no cover
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    def connect(self):
        self.pre_connect_hook()
        if self.db_path is None and self.default_db is not None:
            self.db_path = self.default_db
        self._c = self.connection_pool.get(self.db_path)  # type: ignore
        if self._c is None:
            try:
                self.connection_pool[self.db_path] = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)  # type: ignore
                self._c = self.connection_pool[self.db_path]  # type: ignore
                self._c.row_factory = self.dict_factory
            except TypeError:
                print(f"Connection failed for path: {self.db_path}")
                raise
        self.post_connect_hook()

    @property
    def c(self) -> t.Optional[sqlite3.Connection]:
        if self._c is None:
            self.connect()
        return self._c

    def close(self):
        self.c.close()
        self._c = None
        if self.db_path:
            del self.connection_pool[self.db_path]

    def initialize(self):
        """Create basic db objects."""
        [
            [o.execute() for o in self.information_schema[t].values()]
            for t in ["Pragma", "Table"]
        ]
        return self

    @contextmanager
    def cursor(self, auto_commit=True):
        cursor = self.c.cursor()
        yield cursor
        if auto_commit:
            self.c.commit()
        cursor.close()

    def commit(self):
        self.c.commit()

    def execute(
        self,
        command: t.Union[str, "SQLProvider", "Statement"],
        parameters: t.Union[t.Tuple, t.Dict] = None,
        commit: bool = True,
    ):
        """Execute a sql command with optional parameters"""
        with self.cursor() as cur:
            execute = cur.execute
            sql = ds_sql(command)
            try:
                if ";" in sql:
                    execute = cur.executescript
                else:
                    parameters = (
                        parameters
                        if parameters
                        else (command.data if hasattr(command, "data") else None)
                    )
                    if isinstance(parameters, list):
                        if len(parameters) == 1:
                            parameters = parameters[0]
                        else:
                            execute = cur.executemany
                execute(
                    *[
                        i
                        for i in [
                            sql,
                            parameters,
                        ]
                        if i is not None
                    ]
                )
            except (OperationalError, ValueError, TypeError, ProgrammingError) as e:
                if match := re.search(r"no such table:\s(.*)", str(e)):
                    self.table(match.group(1)).execute()
                    self.execute(command=command, parameters=parameters, commit=commit)
                else:
                    print(f"Syntax error in: {sql}\n\nError: {str(e)}")
                    print(f"Data: {command.data}")
                    raise
            if commit:
                self.commit()
            return cur.fetchall()

    def table(self, name: str) -> "Table":
        table_name = name_parse(name)[1][-1]
        return self.information_schema["Table"][table_name]

    def register(self, object):
        self.information_schema[type(object).__name__][object.name] = object

    def table_exists(self, table_name: str) -> bool:
        return (
            len(
                self.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
                )
            )
            > 0
        )

    @functools.lru_cache
    def id(
        self, table_name: str, column_name: str, column_value: t.Any
    ) -> t.Optional[int]:
        table = self.table(table_name)
        result = table.select(
            column=[table["id"]],
            where={c: column_value for c in listify(table[column_name])},
        ).execute()
        if result is not None and len(result) == 1:
            return result[0]["id"]


# SECTION 3: Type handlers
class TypeHandlerABC(metaclass=ABCMeta):  # pragma: no cover
    @property
    @abstractmethod
    def sql_type(self):
        pass

    @property
    @abstractmethod
    def python_type(self):
        pass

    @staticmethod
    @abstractmethod
    def to_sql(value) -> t.Union[int, float, str, bytes]:
        "This static method should convert a Python data type into one of SQLite’s supported types."
        raise NotImplementedError("Not implemented yet.")

    @staticmethod
    @abstractmethod
    def to_python(value) -> t.Any:
        "This static method should convert a bytestring into the appropriate Python data type."
        raise NotImplementedError("Not implemented yet.")


class TypeHandler(TypeHandlerABC):
    @classmethod
    def register(cls):
        if (
            isinstance(getattr_static(cls, "to_sql"), staticmethod)
            and isinstance(getattr_static(cls, "to_python"), staticmethod)
            and len(cls.__abstractmethods__) == 0  # type: ignore
        ):  # type: ignore
            TypeMaster.register(cls)
        else:
            raise TypeError(
                "Type Handlers must have static methods to_python and to_sql."
            )


class DateHandler(TypeHandler):
    sql_type: str = "INTDATE"
    python_type: type = datetime

    @staticmethod
    def to_sql(value: datetime) -> t.Union[int, float, str, bytes]:
        """Convert datetime to timestamp"""
        return str(value.timestamp())

    @staticmethod
    def to_python(value) -> t.Any:
        if value is not None:
            return datetime.fromtimestamp(value)


class PickleHandler(TypeHandler):
    sql_type: str = "BLOB"

    @staticmethod
    def to_sql(value: datetime) -> str:
        return f"x'{pickle.dumps(value).hex()}'"

    @staticmethod
    def to_python(value) -> t.Any:
        if value is not None:
            return pickle.loads(bytes.fromhex(value))


def pickle_type_handler(object: type):
    type(object.__name__, (PickleHandler,), {"python_type": object}).register()


class Caster:
    """Casters are used in place of handlers where no conversion is needed.
    Casters provide Python Type to SQL Type lookup.
    Should provide a quote_escape static method where appropriate.
    """

    @classmethod
    def register(cls):
        if not hasattr(cls, "sql_type") and hasattr(cls, "python_type"):
            raise AttributeError(
                "Subclasses of Caster must define sql_type and python_Type class attributes."
            )
        TypeMaster.register(cls)


class StrCaster(Caster):
    sql_type: str = "TEXT"
    python_type: type = str

    @staticmethod
    def quote_escape(value) -> str:
        return f"""'{value.strip("'")}'"""


class IntCaster(Caster):
    sql_type: str = "INTEGER"
    python_type: type = int

    @staticmethod
    def quote_escape(value) -> str:
        return str(value)


class FloatHandler(Caster):
    sql_type: str = "REAL"
    python_type: type = float

    @staticmethod
    def quote_escape(value) -> str:
        return str(value)


class TypeMaster:
    type_handlers: t.Dict[t.Type, t.Union[t.Type[TypeHandler], t.Type[Caster]]] = {
        datetime: DateHandler,
        list: PickleHandler,
        dict: PickleHandler,
        tuple: PickleHandler,
        str: StrCaster,
        int: IntCaster,
        float: FloatHandler,
    }
    adaptable: t.Set[t.Type] = set()
    _allow_pickle: bool = False

    @classmethod
    def allow_pickle(cls):
        cls._allow_pickle = True

    @classmethod
    def register(cls, handler: t.Type[TypeHandler]) -> None:
        if hasattr(handler, "to_sql") and callable(getattr(handler, "to_sql")):
            sqlite3.register_adapter(handler.python_type, handler.to_sql)
            cls.adaptable.add(handler.python_type)
        if hasattr(handler, "to_python") and callable(getattr(handler, "to_python")):
            sqlite3.register_converter(handler.sql_type, handler.to_python)
        cls.type_handlers[handler.python_type] = handler  # type: ignore

    @classmethod
    def get(cls, python_type: type) -> t.Callable:
        handler = cls.type_handlers.get(python_type, lambda x: x)
        if not cls._allow_pickle and handler == PickleHandler:
            raise RuntimeError(
                f"Type {python_type} cannot be cast unless Pickling is enabled. Call TypeHandler.allow_pickle() to allow if you understand the security risk."
            )
        return handler

    @classmethod
    def cast(cls, o) -> str:
        if isinstance(o, Column):
            return ds_sql(ds_identity(o))
        if hasattr((caster := cls.get(type(o))), "quote_escape"):
            return caster.quote_escape(o)
        else:
            return o

    def __getitem__(self, key: type) -> t.Type[TypeHandler]:
        return self.type_handlers[key]


# SECTION 4: SQL Component Classes
class TBD:
    pass


@dataclasses.dataclass
class DBObject:
    db_path: t.Optional[str] = dataclasses.field(
        default=None, metadata={"exclude_column": True, "exclude_data": True}
    )

    @property
    def db(self):
        return Database(db_path=self.db_path)

    @db.setter
    def db(self, db):
        self.db_path = db.db_path if hasattr(db, "db_path") else db

    def execute(self) -> t.Optional[t.List]:
        if isinstance(self, SQLProvider):
            return self.db.execute(self)


class SQLProvider(metaclass=ABCMeta):  # pragma: no cover
    @classmethod
    def __subclasshook__(cls, other):
        hookmethod = getattr(other, "sql", None)
        return callable(hookmethod)

    @abstractmethod
    def sql(self):
        pass


class DataProvider(metaclass=ABCMeta):  # pragma: no cover
    @classmethod
    def __subclasshook__(cls, other):
        hookmethod = getattr(other, "data", None)
        return callable(hookmethod)

    @abstractmethod
    def data(self) -> t.Dict:
        pass


@dataclasses.dataclass
class DataClassTable(DataProvider, DBObject):
    _table: t.ClassVar[t.Optional["Table"]] = None
    id: t.Optional[int] = dataclasses.field(
        default=None,
        init=False,
        repr=False,
        metadata={"column_name": "id", "python_type": int, "pkey": True},
    )

    def data(self) -> t.Dict:
        def get_name(field: dataclasses.Field):
            column_name = field.metadata.get("column_name")
            return field.name if column_name is None else column_name

        include = {
            field.name: get_name(field)
            for field in dataclasses.fields(self)
            if not field.metadata.get("exclude_data")
        }
        return {
            include[k]: v for k, v in dataclasses.asdict(self).items() if k in include
        }

    @classmethod
    def get_table(cls):
        if cls._table is None:
            cls._table = tablify(cls)  # type: ignore
        return cls._table

    @property
    def table(self) -> t.Optional["Table"]:
        cls = type(self)
        return cls.get_table()  # type: ignore

    def save(self, replace=True):
        self.table.insert(data=self, replace=replace).execute()
        return self


@dataclasses.dataclass
class RawSQL:
    text: str

    def sql(self) -> str:
        return self.text

    __repr__ = sql
    __identity__ = sql


@dataclasses.dataclass
class Qname:
    db: t.Optional[t.Union[str, Database]] = None
    schema_name: t.Optional[str] = None
    table_name: t.Optional[str] = None
    column_name: t.Optional[str] = None
    alias: t.Optional[str] = None
    python_type: type = str

    def __add__(self, added):
        return Qname(
            **{
                k: v
                for k, v in list(dataclasses.asdict(self).items())
                + list(dataclasses.asdict(added).items())
                if v is not None
            }
        )

    @property
    def parts(self):
        return [
            p
            for p in [self.schema_name, self.table_name, self.column_name]
            if p is not None
        ]

    @property
    def name(self):
        return self.parts[-1]

    def sql(self):
        ident = ".".join([f"[{i}]" for i in self.parts if i is not None])
        if self.alias is not None:
            ident += f" AS {self.alias}"
        return ident if ident is not None else ""

    __repr__ = sql
    identity = sql

    def __hash__(self):
        return hash((*self.parts, self.alias))


@dataclasses.dataclass
class Pragma(DBObject):
    pragma_name: t.Optional[str] = None
    value: t.Optional[str] = None

    @classmethod
    def from_dict(cls, d: t.Dict) -> t.List["Pragma"]:
        return [cls(pragma_name=k, value=v) for k, v in d.items()]

    def sql(self):
        return f"PRAGMA {self.pragma_name}={self.value}"

    @property
    def name(self):
        return self.pragma_name

    def __post_init__(self):
        if self.pragma_name is None or self.value is None:
            raise ValueError("pragma_name and value are required for Pragma instance.")
        self.db.register(self)


@dataclasses.dataclass
class Statement:
    components: t.Dict = dataclasses.field(default_factory=dict, repr=False)
    seperator: str = " "

    def sql(self) -> str:
        for i in self.Order:
            if self.components.get(i) is None:
                if hasattr(self, f"{i.name.lower()}_sql"):
                    getattr(self, f"{i.name.lower()}_sql")()
                elif (kw := i.name.split("_")[0]) in KEYWORDS:
                    self.components[i] = KEYWORDS[kw]

        return self.seperator.join(
            [
                ds_sql(self.components[clause])
                for clause in self.Order
                if self.components.get(clause) is not None
            ]
        )

    @property
    def data(self):
        return None

    @functools.singledispatchmethod
    def get_order(self, key):
        return key

    @get_order.register
    def _(self, key: int) -> Enum:  # type: ignore
        return self.Order(key)

    @get_order.register
    def _(self, key: str) -> Enum:
        return self.Order[key]

    def __getitem__(self, key):
        return self.components[self.get_order(key)]

    def __setitem__(self, key, value):
        key = self.get_order(key)
        if not isinstance(key, self.Order):
            raise ValueError(f"Keys must be {self.__class__.__name__}.Order")
        self.components[key] = value

    class Order(Enum):
        BEGINNING = 1
        MIDDLE = 2
        END = 3


@dataclasses.dataclass
class TableObjectBase(Statement):
    table: t.Optional["Table"] = None

    def from_sql(self):
        self["FROM"] = f"FROM {ds_sql(ds_identity(self.table))}"


@dataclasses.dataclass
class WhereObjectBase(Statement):
    where: WhereLike = dataclasses.field(default_factory=dict)
    _data: t.Dict = dataclasses.field(repr=False, default_factory=dict)

    def where_sql(self):
        w = self.where if hasattr(self.where, "sql") else Where(self.where)
        self["WHERE"] = w.sql()
        self._data = {**self._data, **w.data}

    @property
    def data(self):
        return self._data


@dataclasses.dataclass()
class Insert(DBObject, TableObjectBase):
    replace: bool = False
    column: t.List = dataclasses.field(default_factory=list)  # type: ignore
    _column: t.List = dataclasses.field(init=False, repr=False)
    data: t.List[t.Dict] = dataclasses.field(default_factory=dict)
    _data: t.List[t.Dict] = dataclasses.field(init=False, repr=False)
    _defaults_set: bool = False

    def __post_init__(self):
        if not hasattr(self, "_data"):
            self._data = []

    @property
    def column(self):
        if self._column is None:
            self._column = list()
        return self._column

    @column.setter
    def column(self, column):
        self._column = column

    @property
    def has_defaults(self):
        return any([resolve(c, "default_sig") for c in self.column])

    @property
    def data(self):
        if self._defaults_set is False and self.has_defaults:
            self.set_defaults()
            self._defaults_set = True
        return self._data if self._data else []

    @data.setter
    def data(self, value: t.Union[t.List[t.Dict], t.Dict, DataProvider]) -> None:
        if value is not None:
            self._data = listify(
                value.data() if isinstance(value, DataProvider) else value
            )
            self._defaults_set = False

    def set_defaults(self):
        self._data = [self.add_default(data=d) for d in listify(self._data)]

    def add_default(self, data: t.Dict) -> t.Dict:
        "Adds default values for missing items where column.default_sig."
        result = dict()
        for c in self.column:
            if (value := data.get(c.name)) is None:
                if (sig := c.default_sig) is not None:
                    if sig.parameters.get("data"):
                        value = c.default(data=data)
                    else:
                        value = c.default()
            if value is not None:
                result[c.name] = value
        return result

    def insert_sql(self):
        self["INSERT"] = f"{'REPLACE' if self.replace else 'INSERT'} INTO"

    def identity_sql(self):
        self["IDENTITY"] = self.table.identity

    def column_sql(self):
        if not self.data:
            self["COLUMN"] = "DEFAULT VALUES"
        else:
            self["COLUMN"] = f"({', '.join(self.data[0].keys())})"

    def values_sql(self):
        if self.data:
            self["VALUES"] = f"VALUES ({':'+', :'.join(self.data[0].keys())})"

    class Order(Enum):
        INSERT = 1
        IDENTITY = 2
        COLUMN = 3
        SELECT = 4
        VALUES = 5


@dataclasses.dataclass
class Select(DBObject, WhereObjectBase, TableObjectBase):
    column: t.Optional[t.List[t.Union["Column", "Qname", str, "RawSQL"]]] = None
    _data: t.Dict = dataclasses.field(repr=False, default_factory=dict)

    def __post_init__(self):
        if not hasattr(self, "_data"):
            self._data = []
        self.column = [columnify(c) for c in self.column]
        self["JOIN"] = ClauseList()

    @property
    def data(self):
        return self._data

    def select_sql(self):
        self["SELECT"] = "SELECT"

    def column_sql(self):
        self["COLUMN"] = joinmap([ds_identity(c) for c in self.column], ds_sql)

    def cast(self, column_name: str, value: t.Any) -> t.Any:
        return self.key_casters.get(column_name, lambda x: x)(value)

    def execute(self) -> t.Optional[t.List]:
        return self.db.execute(self)

    def add_column(self, column: t.Union[t.List, str, "Column"]) -> None:
        [self.column.append(columnify(c)) for c in listify(column)]

    def join(
        self,
        join_table,
        columns: t.Optional[t.List] = None,
        on: t.Optional[t.Union["On", t.Dict]] = None,
        keyword="JOIN",
    ) -> "Select":
        if on is None:
            on = self.table.on_from_constraints(join_table)
        elif isinstance(on, dict):
            on = On(where={k: columnify(v) for k, v in on.items()})
        self["JOIN"].add_clause(Join(table=join_table, on=on, keyword=keyword))
        if columns:
            self.add_column(columns)
        return self

    left_join = functools.partialmethod(join, keyword="LEFT JOIN")
    right_join = functools.partialmethod(join, keyword="RIGHT JOIN")
    full_join = functools.partialmethod(join, keyword="FULL OUTER JOIN")

    class Order(Enum):
        CTE = 1
        SELECT = 2
        COLUMN = 3
        FROM = 4
        JOIN = 5
        VALUES = 6
        WHERE = 7
        GROUP = 8
        HAVING = 9
        ORDER = 10
        LIMIT = 11
        OFFSET = 12


@dataclasses.dataclass
class Delete(DBObject, WhereObjectBase, TableObjectBase):
    def delete_sql(self):
        self["DELETE"] = "DELETE"

    class Order(Enum):
        DELETE = 1
        FROM = 2
        WHERE = 3


@dataclasses.dataclass
class Drop(DBObject, TableObjectBase):
    def drop_sql(self):
        self["DROP"] = "DROP TABLE"

    def table_sql(self):
        self["TABLE"] = ds_sql(ds_identity(self.table))

    class Order(Enum):
        DROP = 1
        TABLE = 2


@dataclasses.dataclass
class Column(Statement, DBObject):
    column_name: str = ""
    python_type: type = str
    unique: bool = False
    nullable: bool = True
    pkey: bool = False
    default: t.Any = dataclasses.field(hash=False, default=None)
    table: t.Optional["Table"] = None

    @classmethod
    def from_tuple(cls, t: t.Tuple) -> "Column":
        column_name, detail = t
        if isinstance(detail, type):
            return cls(column_name=column_name, python_type=detail)
        return cls(**{"column_name": column_name, **detail})

    @classmethod
    def id(cls):
        return cls.from_tuple(("id", ID_COLUMN))

    def __post_init__(self):
        if not self.column_name:
            raise ValueError("column_name is required for Column instance.")

    @property
    def name(self):
        return self.column_name

    @property
    def default_sig(self):
        try:
            return signature(self.default)
        except TypeError as e:
            if "is not a callable object" in str(e):
                return None
            else:  # pragma: no cover
                raise e

    @property
    def table_identity(self):
        if self.table:
            return (
                self.table.identity
                if hasattr(self.table, "identity")
                else Qname(table_name=str(self.table))
            )

    @property
    def identity(self):
        ident = Qname(column_name=self.column_name)
        if table_ident := self.table_identity:
            ident = table_ident + ident
        return ident

    def columnname_sql(self):
        self["COLUMNNAME"] = self.column_name

    def type_sql(self):
        self["TYPE"] = TypeMaster.get(self.python_type).sql_type

    def notnull_sql(self):
        if not self.nullable:
            return "NOT NULL"

    def unique_sql(self):
        if self.unique:
            self["UNIQUE"] = "UNIQUE"

    def primarykey_sql(self):
        if self.pkey:
            self["PRIMARYKEY"] = "PRIMARY KEY"

    def default_sql(self):
        if self.default is not None and not self.default_sig:
            self["DEFAULT"] = f"DEFAULT {TypeMaster.cast(self.default)}"

    def __hash__(self):
        return hash((self.table, self.column_name))

    class Order(Enum):
        COLUMNNAME = 1
        TYPE = 2
        NOTNULL = 3
        UNIQUE = 4
        PRIMARYKEY = 5
        DEFAULT = 6

    def __repr__(self):
        return ds_sql(ds_identity(self))


@dataclasses.dataclass
class ForeignKey(Statement):
    column: t.Union[Column, Qname, str, RawSQL] = ""
    reference: t.Union[Column, Qname, str, RawSQL] = ""

    @classmethod
    def from_tuple(
        cls,
        t: t.Tuple[
            t.Union[Column, Qname, str, RawSQL], t.Union[Column, Qname, str, RawSQL]
        ],
    ) -> "ForeignKey":
        return cls(column=t[0], reference=t[1])

    def fkcolumn_sql(self):
        self.column = columnify(self.column)
        self["FKCOLUMN"] = resolve(self.column, ["name", "sql"])

    def reftable_sql(self):
        self.reference = columnify(self.reference)
        self["REFTABLE"] = resolve(
            resolve(self.reference, ["table"]), ["table_name", "name", "sql"]
        )

    def refcolumn_sql(self):
        self.reference = columnify(self.reference)
        self["REFCOLUMN"] = resolve(self.reference, ["column_name", "name", "sql"])

    class Order(Enum):
        FOREIGNKEY = 1
        OPENPAREN = 2
        FKCOLUMN = 3
        CLOSEPAREN = 4
        REFERENCES = 5
        REFTABLE = 6
        OPENPAREN_1 = 7
        REFCOLUMN = 8
        CLOSEPAREN_1 = 9


@dataclasses.dataclass
class UniqueConstraint(Statement):
    column: t.List = dataclasses.field(default_factory=list)

    def column_sql(self):
        self["COLUMN"] = f"{joinmap(self.column, ds_sql)}"

    def unique_sql(self):
        self["UNIQUE"] = "UNIQUE"

    class Order(Enum):
        UNIQUE = 1
        OPENPAREN = 2
        COLUMN = 3
        CLOSEPAREN = 4


@dataclasses.dataclass
class Table(Statement, DBObject):
    table_name: str = ""
    column: t.List = dataclasses.field(default_factory=list)
    constraints: t.List = dataclasses.field(default_factory=list)
    schema_name: t.Optional[str] = None
    alias: t.Optional[str] = None
    reference_data: t.Optional[t.Union[t.Dict, t.List]] = None
    temp: bool = False

    @staticmethod
    def from_object(
        object: t.Union[t.Dict, EnumMeta],
        table_name: str = None,
        reference_data: t.Dict = None,
        db_path: t.Optional[t.Union[str, Database]] = None,
        temp: bool = False,
    ) -> "Table":
        return tablify(
            object,
            table_name=table_name,
            reference_data=reference_data,
            db_path=db_path,
            strict=True,
            temp=temp,
        )  # type: ignore

    @staticmethod
    def from_data(
        data: t.Union[t.Dict, t.List[t.Dict]],
        table_name: str = None,
        db_path: t.Optional[t.Union[str, Database]] = None,
        temp: bool = False,
    ) -> "Table":
        data = listify(data)
        recipe_dict = {k: type(v) for k, v in data[0].items()}
        return tablify(
            recipe_dict,
            table_name=table_name,
            reference_data=data,
            db_path=db_path,
            strict=True,
            temp=temp,
        )  # type: ignore

    @property
    def name(self):
        return self.table_name

    def __post_init__(self):
        if not self.column or not self.table_name:
            raise ValueError("table_name and column are required for Table instance.")
        for c in self.column:
            c.table = self
        self.db.register(self)

    def create_sql(self):
        self[
            "CREATE"
        ] = f"CREATE {'TEMPORARY ' if self.temp else ''}TABLE IF NOT EXISTS {self.name}"

    def column_sql(self):
        self["COLUMN"] = f"{joinmap(self.column, ds_sql)}"

    def constraint_sql(self):
        self[
            "CONSTRAINT"
        ] = f"{',' if self.constraints else ''}{joinmap(self.constraints, ds_sql)}"

    def pkey(self) -> Column:
        return [c for c in self.column if c.pkey][0]

    def fkey(self, on_column: t.Union["Column", "Qname", str, "RawSQL"]) -> ForeignKey:
        return ForeignKey(column=on_column, reference=self.pkey())

    def add_constraint(self, const):
        self.constraints.append(const)

    def on_from_constraints(self, target: "Table") -> "On":
        return On(
            {
                c.column: c.reference
                for c in [
                    c
                    for c in self.constraints
                    if same_table(resolve(resolve(c, "reference"), "table"), target)
                ]
                + [
                    c
                    for c in target.constraints
                    if same_table(resolve(resolve(c, "reference"), "table"), self)
                ]
            }
        )

    @property
    def identity(self):
        return Qname(
            schema_name=self.schema_name, table_name=self.name, alias=self.alias
        )

    def insert(
        self,
        data: t.Optional[t.Union[t.Dict, t.List, DataProvider]],
        column: t.List = None,
        replace: bool = False,
    ) -> Insert:
        return Insert(
            data=data,
            column=column if column else self.column,
            replace=replace,
            table=self,
            db_path=self.db_path,
        )

    def select(self, where: WhereLike = None, column: t.List = None) -> Select:
        return Select(
            where=where if where is not None else Where(),
            column=column if column else self.column,
            table=self,
            db_path=self.db_path,
        )

    def delete(self, where: WhereLike) -> Delete:
        return Delete(table=self, where=where, db_path=self.db_path)

    def drop(self, confirm: bool = False):
        if self.temp is False and not confirm:
            raise OperationalError(
                "Cannot drop a non-temporary table without confirm flag set to True."
            )
        return Drop(table=self)

    def insert_ref_data(self):
        if self.reference_data is not None:
            self.insert(data=self.reference_data, replace=True).execute()

    def execute(self):
        self.db.execute(self)
        self.insert_ref_data()

    def __repr__(self):
        return ds_sql(ds_identity(self.identity))

    def __getitem__(self, key):
        return [c for c in self.column if c.name == key][0]

    def __hash__(self):
        return hash((self.schema_name, self.table_name))

    class Order(Enum):
        CREATE = 1
        OPENPAREN = 2
        COLUMN = 3
        CONSTRAINT = 4
        CLOSEPAREN = 5


def ue_id():
    "Unique Enough Id"
    return b32encode(os.urandom(5)).decode("utf-8")


@dataclasses.dataclass
class Comparison:
    column: t.Union[
        t.Union["Column", "Qname", str, t.Type["TBD"], "RawSQL"], t.Type[TBD]
    ]
    target: t.Union[Statement, str, t.Type[TBD]]
    operator: t.Literal["=", ">", "<", "!=", "<>", ">=", "<="]
    key: str = dataclasses.field(default_factory=ue_id)

    @property
    def data(self):
        return {self.key: self.target}

    def sql(self):
        if self.column == TBD or self.column is None:
            raise TypeError("Column argument is required")
        if isinstance(self.target, (Column, Qname)):
            t = ds_sql(ds_identity(self.target))
        else:
            t = f":{self.key}"
        return f"{ds_sql(ds_identity(self.column))} {self.operator} {t}"

    @classmethod
    def get_comparison(
        cls,
        column: t.Union["Column", "Qname", str, t.Type["TBD"], "RawSQL"] = TBD,
        target: Statement = None,
        operator: t.Literal["=", ">", "<", "!=", "<>", ">=", "<="] = "=",
        key: str = None,
    ) -> "Comparison":
        if target is None:
            raise TypeError("target argument is required")
        return cls(
            column=columnify(column), target=target, operator=operator, key=key if key else ue_id()  # type: ignore
        )

    equal = eq = functools.partialmethod(get_comparison, operator="=")
    not_equal = ne = functools.partialmethod(get_comparison, operator="!=")
    greater_than = gt = functools.partialmethod(get_comparison, operator=">")
    greater_than_or_equal = gtoe = functools.partialmethod(
        get_comparison, operator=">="
    )
    less_than = lt = functools.partialmethod(get_comparison, operator="<")
    less_than_or_equal = ltoe = functools.partialmethod(get_comparison, operator="<=")
    like = functools.partialmethod(get_comparison, operator="LIKE")

    @dataclasses.dataclass
    class In(Statement):
        column: t.Union[Statement, t.Type[TBD]] = TBD
        target: t.Union[Statement, t.Type[TBD]] = TBD
        invert: bool = False

        def sql(self):
            return f"""{ds_identity(self.column)} {"NOT" if self.invert else ""} IN ({joinmap(self.target, TypeMaster.cast)})"""

    @classmethod
    def is_in(
        cls,
        column: t.Union[Statement, t.Type[TBD]] = TBD,
        target: Statement = None,
        invert: bool = False,
    ) -> "In":
        if target is None:
            raise TypeError("target argument is required")
        return cls.In(column=column, target=target, invert=invert)

    not_in = functools.partialmethod(is_in, invert=True)


@dataclasses.dataclass
class Where:
    where: WhereLike = dataclasses.field(default_factory=dict)
    keyword: str = "WHERE"
    seperator: str = LINE + TAB + "AND "
    prefix: str = ""
    suffix: str = ""
    _data: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)

    def __getitem__(self, key):
        return self.where[key]

    def __setitem__(self, key, value):
        self.where[key] = value

    @property
    def data(self):
        return self._data

    def nest(self, keyword=""):
        self.keyword = keyword
        self.prefix = "("
        self.suffix = ")"

    def sql(self):
        if not self.where:
            return ""
        clause_list = ClauseList(
            seperator=self.seperator, prefix=self.prefix, suffix=self.suffix
        )
        for k, v in self.where.items():
            if isinstance(v, Where):
                v.nest(keyword=k)
            else:
                if (
                    isinstance(v, (str, int, float, Column, Qname))
                    or type(v) in TypeMaster.adaptable
                ):
                    v = Comparison.get_comparison(column=k, target=v)  # type: ignore
                if hasattr(v, "column") and v.column == TBD:  # type: ignore
                    v.column = columnify(k)  # type: ignore
            clause_list += v

        sql = clause_list.sql()
        self._data = clause_list.data
        return self.keyword + " " + sql

    def items(self):
        return self.where.items()


@dataclasses.dataclass
class On(Where):
    keyword: str = "ON"


@dataclasses.dataclass
class Join(DBObject, TableObjectBase):
    on: t.Optional[WhereLike] = None
    keyword: str = "JOIN"

    @property
    def seperator(self):
        return " "

    def join_sql(self) -> str:
        return f"{self.keyword} "

    def table_sql(self) -> str:
        self["TABLE"] = ds_sql(ds_identity(self.table))

    def on_sql(self) -> str:
        if isinstance(self.on, dict):
            self.on = On(self.on)
        self["ON"] = ds_sql(self.on)

    class Order(Enum):
        JOIN = 1
        TABLE = 2
        ON = 3


@dataclasses.dataclass
class ClauseList:
    clauses: t.List = dataclasses.field(default_factory=list)
    keyword: str = ""
    seperator: str = LINE
    prefix: str = ""
    suffix: str = ""
    _data: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict, repr=False)

    @property
    def data(self):
        return self._data

    def harvest(self, c: t.Union[DataProvider, SQLProvider], i: int):
        result = resolve(c, "sql")
        if (data := resolve(c, "data")) and isinstance(data, dict):
            self._data = {**self._data, **data}
        if hasattr(c, "keyword"):
            keyword = " "
        else:
            keyword = self.seperator if i > 0 else ""
        return keyword + result

    def sql(self):
        return (
            self.prefix
            + "".join([self.harvest(c, i) for i, c in enumerate(self.clauses)])
            + self.suffix
        )

    def add_clause(self, clause):
        self.clauses.append(clause)

    def __iadd__(self, o: t.Union[DataProvider, SQLProvider]):
        self.clauses.append(o)
        return self


# SECTION 5: Utility functions
def resolve(o: t.Any, attrs: t.Any):
    for a in listify(attrs):
        try:
            attr = getattr(o, a)
            if callable(attr):
                return attr()
            else:
                return attr
        except AttributeError:
            pass
    return o


ds_name = functools.partial(resolve, attrs="name")
ds_identity = functools.partial(resolve, attrs="identity")
ds_sql = functools.partial(resolve, attrs="sql")


@functools.lru_cache
def enum_to_id(e: Enum) -> int:
    return list(type(e).__members__.keys()).index(e.name)


def id_to_enum_member(id: int, e: EnumMeta) -> Enum:
    return list(e.__members__.values())[id]  # type: ignore


def enum_type_handler(e: EnumMeta):
    """Enumerations are an efficient representation of a sql lookup table;
    it is possible to make predictable primary keys, avoiding lookups.
    """
    if (
        len((enum_types := list(set([type(member.value) for member in e])))) == 1
        and enum_types[0] == int
    ):

        @staticmethod
        def to_sql(value) -> str:
            return str(value.value)

        @staticmethod
        def to_python(value):
            return e(int(value))

    else:

        @staticmethod
        def to_sql(value) -> str:
            return str(enum_to_id(value))

        @staticmethod
        def to_python(value) -> t.Any:
            return id_to_enum_member(value, e)

    AutoEnumTypeHandler = type(
        f"{e.__name__}TypeHandler",
        (TypeHandler,),
        {
            "sql_type": e.__name__,
            "python_type": e,
            "to_sql": to_sql,
            "to_python": to_python,
            "quote_escape": to_sql,
        },
    )
    AutoEnumTypeHandler.register()


def table_to_enum(
    table, enum_name=None, name_column="name", value_column="value"
) -> EnumMeta:
    table = tablify(table, strict=True)
    if enum_name is None:
        enum_name = table.name
    data = table.select(column=[name_column, value_column]).execute()
    if not data:
        raise ValueError(f"No data in {table}.")
    return Enum(enum_name, {row[name_column]: row[value_column] for row in data})


def name_parse(object: str) -> t.Tuple[t.Optional[str], t.List[str]]:
    split = [p for p in re.split(r"\s", object) if p.lower() != "as"]
    if len(split) > 1:
        alias = split.pop()
    else:
        alias = None
    parts = [re.sub(r"[\[\]]", "", p) for p in re.split(r"\.", split[0])]
    return (alias, parts)


@functools.singledispatch
def tablify(
    object,
    table_name: str = None,
    reference_data: t.Optional[t.Dict] = None,
    db_path: Database = None,
    temp: bool = False,
    strict: bool = False,
):
    if dataclasses.is_dataclass(object):
        return tablify(type(object))
    if not isinstance(object, (Table, Qname, EnumMeta, t.Dict, RawSQL)):
        raise ValueError(
            "Only instances of str, Table, Qname, dict, and RawSQL, or subclasses of Enum are valid."
        )
    if strict and not isinstance(object, (Table, Enum, t.Dict)):
        raise ValueError(
            f"{type(object)} type object cannot represent a table in strict contexts."
        )
    return object


@tablify.register
def _(  # type: ignore
    object: type,
    table_name: str = None,
    reference_data: t.Optional[t.Dict] = None,
    db_path: str = None,
    temp: bool = False,
    strict: bool = False,
) -> "Table":
    if db_path is not None:
        db = Database(db_path=db_path)
        if (retrieved := db.table(object.__name__)) is not None:
            return retrieved
    if not dataclasses.is_dataclass(object):
        raise ValueError(f"{type(object)} is not a supported type.")
    return Table(
        table_name=object.__name__,
        column=[
            columnify(field)
            for field in dataclasses.fields(object)
            if not field.metadata.get("exclude_column")
        ],
        temp=temp,
    )


@tablify.register
def _(  # type: ignore
    object: EnumMeta,
    table_name: str = None,
    reference_data: t.Optional[t.Dict] = None,
    db_path: t.Optional[str] = None,
    temp: bool = False,
    strict: bool = False,
) -> "Table":
    enum_type_handler(object)
    return Table(
        table_name=table_name if table_name else object.__name__,
        column=[
            Column(column_name="id", python_type=int, pkey=True),
            Column(column_name="name", unique=True),
            Column(column_name="value"),
        ],
        reference_data=[
            {"id": member, "name": member.name, "value": member.value} for member in object  # type: ignore
        ],
        db_path=db_path,
        temp=temp,
    )


@tablify.register
def _(  # type: ignore
    object: dict,
    table_name: str = None,
    reference_data: t.Optional[t.Dict] = None,
    db_path: t.Optional[str] = None,
    temp: bool = False,
    strict: bool = False,
) -> Table:
    kwargs = {
        "table_name": table_name
        if table_name is not None
        else object.pop("table_name", None),
        "reference_data": reference_data
        if reference_data is not None
        else object.pop("reference_data", None),
        "db_path": db_path if db_path is not None else object.pop("db_path", None),
        "constraints": object.pop("constraints", None),
        "temp": temp,
    }
    if "column" not in object and kwargs.get("reference_data") is not None:
        object = {
            **{"id": ID_COLUMN},
            **{k: type(v) for k, v in listify(kwargs.get("reference_data"))[0].items()},
        }
    return Table(
        **{k: v for k, v in kwargs.items() if v is not None},
        column=[Column.from_tuple(i) for i in object.items()],
    )


@tablify.register
def _(  # type: ignore
    object: str,
    table_name: str = None,
    reference_data: t.Optional[t.Dict] = None,
    db_path: Database = None,
    strict: bool = False,
) -> t.Union[Qname, Table]:
    if (table := Database().table(object)) is not None:
        return table
    elif strict:
        raise ValueError("str type object cannot represent a table in strict contexts.")
    alias, schema_name = None, None
    alias, parts = name_parse(object)
    if (length := len(parts)) == 2:
        schema_name, table_name = parts
    if length == 1:
        table_name = parts[0]
    return Qname(
        alias=alias, schema_name=schema_name, table_name=table_name, db_path=db_path
    )


@functools.singledispatch
def columnify(object):
    if object != TBD and not isinstance(object, (Column, Qname, RawSQL)):
        raise ValueError(
            "Only instances of str, Column, Qname, TBD, dataclasses.Field, and RawSQL are valid."
        )
    return object


@columnify.register
def _(object: str) -> Qname:  # type: ignore
    alias, schema_name, table_name, column_name = None, None, None, None
    alias, parts = name_parse(object)
    if (length := len(parts)) == 3:
        schema_name, table_name, column_name = parts
    if length == 2:
        table_name, column_name = parts
    if length == 1:
        column_name = parts[0]
    return Qname(
        alias=alias,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
    )


@columnify.register
def _(object: dataclasses.Field) -> Column:  # type: ignore
    parameters = inspect.signature(Column).parameters.keys()
    default = (
        object.default
        if object.default != dataclasses.MISSING
        else object.default_factory
        if object.default_factory != dataclasses.MISSING
        else None
    )
    python_type = (
        object.type
        if t.get_origin(object.type) is None
        else [t for t in t.get_args(object.type) if t is not None][0]
    )
    kwargs = {
        **{"column_name": object.name, "python_type": python_type, "default": default},
        **{k: v for k, v in object.metadata.items() if k in parameters},
    }
    return Column(**kwargs)


def listify(o: t.Any):
    if o is None:
        return []
    return o if isinstance(o, list) else [o]


def joinmap(o, f: t.Callable = ds_name, seperator: str = ", ") -> str:
    """Returns a seperated list of f(i) for i in o."""
    return seperator.join([str(f(o)) for o in listify(o)])


@functools.singledispatch
def table_ident(object) -> t.Any:
    return object  # type: ignore


@table_ident.register
def _(object: str) -> Qname:
    return tablify(object)  # type: ignore


@table_ident.register
def _(object: Table) -> Qname:  # type: ignore
    return object.identity


def same_table(
    a: t.Union[str, Table, Qname, RawSQL], b: t.Union[str, Table, Qname, RawSQL]
) -> bool:
    def lo(object):
        return resolve(object, "lower")

    a, b, match = table_ident(a), table_ident(b), True
    if not hasattr(a, "schema_name") or not hasattr(b, "schema_name"):
        match = False
    elif (
        a.schema_name is not None
        and b.schema_name is not None
        and lo(resolve(b, "schema_name")) != lo(resolve(a, "schema_name"))
    ):
        match = False
    if lo(resolve(a, "table_name")) != lo(resolve(b, "table_name")):
        match = False
    if not isinstance((a_str := lo(ds_sql(ds_identity(a)))), str) or not isinstance(
        (b_str := lo(ds_sql(ds_identity(b)))), str
    ):
        match = False
    elif re.sub(r"\s+", " ", a_str).strip() == re.sub(r"\s+", " ", b_str).strip():
        match = True
    return match


def make_table(cls):
    tablify(cls)
    return cls


def hook_setter(run_once=True, attribute=""):  # pragma: no cover
    def outer_wrapper(func):
        @functools.wraps(func)
        def f(*args):
            func(args[0])
            if run_once:
                setattr(Database, attribute, lambda x: x)

        setattr(Database, attribute, f)

    return outer_wrapper


pre_connect = functools.partial(hook_setter, attribute="pre_connect_hook")
post_connect = functools.partial(hook_setter, attribute="post_connect_hook")
