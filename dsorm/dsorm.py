"""
D.S.O: Darned Simple ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""

import dataclasses
import functools
import pickle
import re
import sqlite3
import typing as t
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from enum import Enum, EnumMeta
from inspect import getattr_static, signature
from sqlite3.dbapi2 import OperationalError


# SECTION 1: Types / Literals
WhereLike = t.Union["Where", t.Dict]
LINE: str = "\n"
TAB = "\t"
ID_COLUMN = {"python_type": int, "pkey": True}
KW = {
    "OPENPAREN": "(",
    "CLOSEPAREN": ")",
    "FOREIGNKEY": "FOREIGN KEY",
    "REFERENCES": "REFERENCES",
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
    def from_dict(cls, d: t.Dict) -> "Database":
        db = cls(db_path=d.get("db_path", ":memory:"))
        tables = {
            table_name: Table.from_object(object, table_name=table_name, db=db)
            for table_name, object in d.get("tables", {}).items()
        }
        for ct in d.get("constraints", {}).items():
            ct = tuple([i for i in ct if isinstance(i, (Column, str, Qname, RawSQL))])
            tables[resolve(columnify(ct[0]), "table_name")].add_constraint(
                ForeignKey.from_tuple(ct)
            )
        db.init_db()
        [tables[n].insert(d).execute() for n, d in d.get("data", {}).items()]
        return db

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
                self.connection_pool[self.db_path] = sqlite3.connect(self.db_path)  # type: ignore
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

    def init_db(self):
        """ Create basic db objects. """
        [
            [o.execute() for o in self.information_schema[t].values()]
            for t in ["Pragma", "Table"]
        ]

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
        command: t.Union[str, "SQL"],
        parameters: t.Union[t.Tuple, t.Dict] = None,
        commit: bool = True,
    ):
        """ Execute a sql command with optional parameters """
        with self.cursor() as cur:
            try:
                if ";" in (sql := ds_sql(command)):
                    cur.executescript(sql)
                else:
                    cur.execute(*[i for i in [sql, parameters] if i is not None])
            except (OperationalError, ValueError) as e:
                print(f"Syntax error in: {ds_sql(command)}\n\nError: {str(e)}")
                raise
            if commit:
                self.commit()
            return cur.fetchall()

    def table(self, name: str) -> "Table":
        return self.information_schema["Table"][name]

    def register(self, object):
        self.information_schema[type(object).__name__][object.name] = object

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
    def p2s(value) -> t.Union[int, float, str, bytes]:
        """ This method should return a value that would be valid "as is" in a sql statement. """
        raise NotImplementedError("Not implemented yet.")

    @staticmethod
    @abstractmethod
    def s2p(value) -> t.Any:
        """ This method should handle converting a SQLite datatype to a Python datatype. """
        raise NotImplementedError("Not implemented yet.")


class TypeHandler(TypeHandlerABC):
    @classmethod
    def register(cls):
        if (
            isinstance(getattr_static(cls, "p2s"), staticmethod)
            and isinstance(getattr_static(cls, "s2p"), staticmethod)
            and len(cls.__abstractmethods__) == 0  # type: ignore
        ):  # type: ignore
            TypeMaster.register(cls)
        else:
            raise TypeError("Type Handlers must have static methods s2p and p2s.")


class StrHandler(TypeHandler):
    sql_type: str = "TEXT"
    python_type: type = str

    @staticmethod
    def p2s(value) -> t.Union[int, float, str, bytes]:
        """ surround with single quotes to embed string literals in sql """
        return f"'{str(value)}'"

    @staticmethod
    def s2p(value) -> t.Any:
        return str(value)


class IntHandler(TypeHandler):
    sql_type: str = "INTEGER"
    python_type: type = int

    @staticmethod
    def p2s(value) -> t.Union[int, float, str, bytes]:
        """ surround with single quotes to embed string literals in sql """
        return str(value)

    @staticmethod
    def s2p(value) -> t.Any:
        if value is not None:
            return int(value)


class FloatHandler(TypeHandler):
    sql_type: str = "REAL"
    python_type: type = float

    @staticmethod
    def p2s(value) -> t.Union[int, float, str, bytes]:
        return str(value)

    @staticmethod
    def s2p(value) -> t.Any:
        if value is not None:
            return float(value)


class DateHandler(TypeHandler):
    sql_type: str = "INT"
    python_type: type = datetime

    @staticmethod
    def p2s(value: datetime) -> t.Union[int, float, str, bytes]:
        """ Convert datetime to timestamp """
        return str(value.timestamp())

    @staticmethod
    def s2p(value) -> t.Any:
        if value is not None:
            return datetime.fromtimestamp(value)


@staticmethod
def obj_2_sql(value: datetime) -> str:
    """ Convert datetime to timestamp """
    return f"x'{pickle.dumps(value).hex()}'"


@staticmethod
def sql_2_obj(value) -> t.Any:
    if value is not None:
        return pickle.loads(bytes(value))


class PickleHandler(TypeHandler):
    sql_type: str = "BLOB"
    p2s = obj_2_sql
    s2p = sql_2_obj


class TypeMaster:
    type_handlers: t.Dict[t.Type, t.Type[TypeHandler]] = {
        str: StrHandler,
        int: IntHandler,
        float: FloatHandler,
        datetime: DateHandler,
        list: PickleHandler,
        dict: PickleHandler,
        tuple: PickleHandler,
    }
    _allow_pickle: bool = False

    @classmethod
    def allow_pickle(cls):
        cls._allow_pickle = True

    @classmethod
    def register(cls, handler: t.Type[TypeHandler]) -> None:
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
        return cls.get(type(o)).p2s(o)

    def __getitem__(self, key: type) -> t.Type[TypeHandler]:
        return self.type_handlers[key]


# SECTION 4: SQL Component Classes
class TBD:
    pass


@dataclasses.dataclass
class DBObject:
    db_path: t.Optional[str] = None
    _db: t.Optional["Database"] = dataclasses.field(repr=False, default=None)

    @property
    def db(self):
        if self._db is None:
            self._db = Database(db_path=self.db_path)
        return self._db

    @db.setter
    def db(self, db):
        self._db = db

    def execute(self) -> t.Optional[t.List]:
        if isinstance(self, SQL):
            return self.db.execute(self)


class SQL(metaclass=ABCMeta):  # pragma: no cover
    @abstractmethod
    def sql(self):
        pass


@dataclasses.dataclass
class RawSQL(SQL):
    text: str

    def sql(self) -> str:
        return self.text

    __repr__ = sql
    __identity__ = sql


@dataclasses.dataclass
class Qname(SQL):
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
class Pragma(DBObject, SQL):
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
class Statement(SQL):
    components: t.Dict = dataclasses.field(default_factory=dict, repr=False)

    @property
    def component_seperator(self):
        return " "

    def sql(self) -> str:
        for i in self.Order:
            if i.name.startswith("KW_"):
                self.components[i] = KW[i.name.split("_")[1]]
            elif self.components.get(i) is None:
                try:
                    if (result := getattr(self, f"{i.name.lower()}_sql")()) is not None:
                        self[i] = result
                except AttributeError:
                    pass

        return self.component_seperator.join(
            [
                ds_sql(self.components[clause])
                for clause in self.Order
                if self.components.get(clause) is not None
            ]
        )

    @functools.singledispatchmethod
    def get_order(self, key):
        return key

    @get_order.register
    def _(self, key: int) -> Enum:  # type: ignore
        return self.Order(key)

    @get_order.register
    def _(self, key: str) -> Enum:
        return self.Order[key]

    def ommit(self, key):
        self[self.get_order(key)] = ""

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

    def where_sql(self):
        self["WHERE"] = self.where if hasattr(self.where, "sql") else Where(self.where)


@dataclasses.dataclass
class Insert(DBObject, TableObjectBase):
    data: t.Optional[t.Union[t.Dict, t.List]] = dataclasses.field(default_factory=dict)
    _prepared_data: t.Optional[t.List] = None
    replace: bool = False
    column: t.List = dataclasses.field(default_factory=list)  # type: ignore
    _column: t.List = dataclasses.field(init=False, repr=False)

    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column = column

    def prepared_data(self):
        if self.data and self._prepared_data is None:
            self._prepared_data = [self.data_prep(data=d) for d in listify(self.data)]
        return self._prepared_data

    def data_prep(self, data: t.Dict) -> t.Dict:
        """Returns a dictionary ready for use in values statment.
        Column order ensures multiple value rows have the same order.
        Adds default values for missing items where column.default_sig.
        Applies quoting rules to values to make them SQL ready
        """
        result = dict()
        for c in self.column:
            if (value := data.get(c.name)) is None:
                if (sig := c.default_sig) is not None:
                    if sig.parameters.get("data"):
                        value = c.default(data=data)
                    else:
                        value = c.default()
            if value is not None:
                result[c.name] = str(TypeMaster.cast(value))
        return result

    def insert_sql(self):
        self["INSERT"] = f"{'REPLACE' if self.replace else 'INSERT'} INTO"

    def identity_sql(self):
        return self.table.identity

    def column_sql(self):
        if self.data is None or (d := self.prepared_data()) is None:
            self["COLUMN"] = "DEFAULT VALUES"
        else:
            self["COLUMN"] = f"({', '.join(d[0].keys())})"

    def values_sql(self):
        if self.data is not None:
            self[
                "VALUES"
            ] = f"""VALUES {", ".join([f"({', '.join(d.values())})" for d in self.prepared_data()])}"""

    class Order(Enum):
        INSERT = 1
        IDENTITY = 2
        COLUMN = 3
        SELECT = 4
        VALUES = 5


@dataclasses.dataclass
class Select(DBObject, WhereObjectBase, TableObjectBase):
    column: t.Optional[t.List[t.Union["Column", "Qname", str, "RawSQL"]]] = None

    def __post_init__(self):
        self.column = [columnify(c) for c in self.column]
        self.key_casters = {
            c.name: TypeMaster.get(c.python_type).s2p  # type: ignore
            for c in self.column
            if hasattr(c, "name")
        }
        self["JOIN"] = Clauses()

    def select_sql(self):
        self["SELECT"] = "SELECT"

    def column_sql(self):
        self["COLUMN"] = joinmap([ds_identity(c) for c in self.column], ds_sql)

    def cast(self, column_name: str, value: t.Any) -> t.Any:
        return self.key_casters.get(column_name, lambda x: x)(value)

    def execute(self) -> t.Optional[t.List]:
        return [
            {k: self.cast(k, v) for k, v in d.items()} for d in self.db.execute(self)
        ]

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
            [self.add_column(c) for c in columns]
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
        if (table_ident := self.table_identity) :
            ident = table_ident + ident
        return ident

    def columnname_sql(self):
        return self.column_name

    def type_sql(self):
        return TypeMaster.get(self.python_type).sql_type

    def notnull_sql(self):
        if not self.nullable:
            return "NOT NULL"

    def unique_sql(self):
        if self.unique:
            return "UNIQUE"

    def primarykey_sql(self):
        if self.pkey:
            return "PRIMARY KEY"

    def default_sql(self):
        if self.default is not None and not self.default_sig:
            return f"DEFAULT {TypeMaster.cast(self.default)}"

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
        return resolve(self.reference, ["column_name", "name", "sql"])

    class Order(Enum):
        KW_FOREIGNKEY = 1
        KW_OPENPAREN_1 = 2
        FKCOLUMN = 3
        KW_CLOSEPAREN_1 = 4
        KW_REFERENCES = 5
        REFTABLE = 6
        KW_OPENPAREN_2 = 7
        REFCOLUMN = 8
        KW_CLOSEPAREN_3 = 9


@dataclasses.dataclass
class Table(Statement, DBObject):
    table_name: str = ""
    column: t.List = dataclasses.field(default_factory=list)
    constraints: t.List = dataclasses.field(default_factory=list)
    schema_name: t.Optional[str] = None
    alias: t.Optional[str] = None
    refdata: dataclasses.InitVar[t.Optional[t.Union[t.Dict, t.List]]] = None

    @staticmethod
    def from_object(
        object: t.Union[t.Dict, EnumMeta],
        table_name: str = None,
        refdata: t.Dict = None,
        db: t.Optional[t.Union[str, Database]] = None,
    ) -> "Table":
        return tablify(
            object, table_name=table_name, refdata=refdata, db=db, strict=True
        )  # type: ignore

    @property
    def name(self):
        return self.table_name

    def __post_init__(self, refdata):
        if not self.column or not self.table_name:
            raise ValueError("table_name and column are required for Table instance.")
        for c in self.column:
            c.table = self
        self.db.register(self)
        if refdata is not None:
            self.components[self.Order["REFDATA"]] = self.insert(data=refdata, replace=True)

    def create_sql(self):
        return f"CREATE TABLE IF NOT EXISTS {self.name}"

    def column_sql(self):
        return f"{joinmap(self.column, ds_sql)}"

    def constraint_sql(self):
        return f"{',' if len(self.constraints) > 0 else ''}{joinmap(self.constraints, ds_sql)}"

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
        data: t.Optional[t.Union[t.Dict, t.List]],
        column: t.List = None,
        replace: bool = False,
    ) -> Insert:
        return Insert(
            data=data,
            column=column if column else self.column,
            replace=replace,
            table=self,
            _db=self.db,
        )

    def select(self, where: WhereLike = None, column: t.List = None) -> Select:
        return Select(
            where=where if where is not None else Where(),
            column=column if column else self.column,
            table=self,
            _db=self.db,
        )

    def delete(self, where: WhereLike) -> Delete:
        return Delete(table=self, where=where, _db=self.db)

    def __repr__(self):
        return ds_sql(ds_identity(self.identity))

    def __getitem__(self, key):
        return [c for c in self.column if c.name == key][0]

    def __hash__(self):
        return hash((self.schema_name, self.table_name))

    class Order(Enum):
        CREATE = 1
        KW_OPENPAREN = 2
        COLUMN = 3
        CONSTRAINT = 4
        KW_CLOSEPAREN = 5
        KW_TERMINATOR_1 = 6
        REFDATA = 7


@dataclasses.dataclass
class Where(SQL):
    where: WhereLike = dataclasses.field(default_factory=dict)
    keyword: str = "WHERE"

    def __getitem__(self, key):
        return self.where[key]

    def __setitem__(self, key, value):
        self.where[key] = value

    def sql(self):
        if not self.where:
            return ""
        clause_list = list()
        extras = list()
        for k, v in self.where.items():
            if isinstance(v, Where):
                v.keyword = ""
                extras.append(f"{LINE + str(k)} ({ds_sql(v) + LINE})")
            else:
                if isinstance(v, (str, int, float)):
                    v = self.get_comparison(column=k, target=v)  # type: ignore
                if hasattr(v, "column") and v.column == TBD:  # type: ignore
                    v.column = columnify(k)  # type: ignore
                if isinstance(v, (Column, Qname)):
                    v = self.get_comparison(k, v)  # type: ignore
                clause_list.append(v)
        return f"""{self.keyword} {joinmap(clause_list, ds_sql, seperator=LINE + TAB + "AND ")}{joinmap(extras, seperator=LINE)}"""

    def items(self):
        return self.where.items()

    @dataclasses.dataclass
    class Comparison:
        column: t.Union[
            t.Union["Column", "Qname", str, t.Type["TBD"], "RawSQL"], t.Type[TBD]
        ]
        target: t.Union[Statement, str, t.Type[TBD]]
        operator: t.Literal["=", ">", "<", "!=", "<>", ">=", "<="]

        def sql(self):
            if self.column == TBD or self.column is None:
                raise TypeError("Column argument is required")
            if isinstance(self.target, (Column, Qname)):
                t = ds_sql(ds_identity(self.target))
            else:
                t = TypeMaster.cast(self.target)
            return f"{ds_sql(ds_identity(self.column))} {self.operator} {ds_sql(ds_identity(t))}"

    @classmethod
    def get_comparison(
        cls,
        column: t.Union["Column", "Qname", str, t.Type["TBD"], "RawSQL"] = TBD,
        target: Statement = None,
        operator: t.Literal["=", ">", "<", "!=", "<>", ">=", "<="] = "=",
    ) -> "Where.Comparison":
        if target is None:
            raise TypeError("target argument is required")
        return cls.Comparison(
            column=columnify(column), target=target, operator=operator  # type: ignore
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
    ) -> "Where.In":
        if target is None:
            raise TypeError("target argument is required")
        return cls.In(column=column, target=target, invert=invert)

    not_in = functools.partialmethod(is_in, invert=True)


@dataclasses.dataclass
class On(Where):
    keyword: str = "ON"


@dataclasses.dataclass
class Join(DBObject, TableObjectBase):
    on: t.Optional[WhereLike] = None
    keyword: str = "JOIN"

    @property
    def component_seperator(self):
        return " "

    def join_sql(self) -> str:
        return f"{self.keyword} "

    def table_sql(self) -> str:
        return ds_sql(ds_identity(self.table))

    def on_sql(self) -> str:
        if isinstance(self.on, dict):
            self.on = On(self.on)
        return ds_sql(self.on)

    class Order(Enum):
        JOIN = 1
        TABLE = 2
        ON = 3


@dataclasses.dataclass
class Clauses(SQL):
    clauses: t.List = dataclasses.field(default_factory=list)
    seperator: str = LINE

    def sql(self):
        return self.seperator.join([ds_sql(i) for i in self.clauses])

    def add_clause(self, clause):
        self.clauses.append(clause)


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
        def p2s(value) -> str:
            return str(value.value)

        @staticmethod
        def s2p(value) -> type(e):
            return e(int(value))

    else:

        @staticmethod
        def p2s(value) -> str:
            return str(enum_to_id(value))

        @staticmethod
        def s2p(value) -> t.Any:
            return id_to_enum_member(value, e)

    AutoEnumTypeHandler = type(
        f"{type(e).__name__}TypeHandler",
        (TypeHandler,),
        {
            "sql_type": "INTEGER",
            "python_type": e,
            "p2s": p2s,
            "s2p": s2p,
        },
    )
    AutoEnumTypeHandler.register()


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
    refdata: t.Optional[t.Dict] = None,
    db: Database = None,
    strict: bool = False,
):
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
    object: EnumMeta,
    table_name: str = None,
    refdata: t.Optional[t.Dict] = None,
    db: Database = None,
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
        refdata=[
            {"id": member, "name": member.name, "value": member.value} for member in object  # type: ignore
        ],
        _db=db,
    )


@tablify.register
def _(  # type: ignore
    object: dict,
    table_name: str = None,
    refdata: t.Optional[t.Dict] = None,
    db: Database = None,
    strict: bool = False,
) -> Table:
    if table_name is None and "table_name" in object:
        table_name = object.pop("table_name")
    if table_name is None:
        raise ValueError("table_name is required")
    if refdata is None and "refdata" in object:
        refdata = object.pop("refdata")
    if db is None:
        db = object.pop("db", None)
    if "column" not in object and refdata is not None:
        object = {
            **{"id": ID_COLUMN},
            **{k: type(v) for k, v in listify(refdata)[0].items()},
        }
    return Table(
        table_name=table_name,
        column=[Column.from_tuple(i) for i in object.items()],
        refdata=refdata,
        _db=db,
    )


@tablify.register
def _(  # type: ignore
    object: str,
    table_name: str = None,
    refdata: t.Optional[t.Dict] = None,
    db: Database = None,
    strict: bool = False,
) -> Qname:
    if strict:
        raise ValueError("str type object cannot represent a table in strict contexts.")
    alias, schema_name = None, None
    alias, parts = name_parse(object)
    if (length := len(parts)) == 2:
        schema_name, table_name = parts
    if length == 1:
        table_name = parts[0]
    return Qname(alias=alias, schema_name=schema_name, table_name=table_name, db=db)


@functools.singledispatch
def columnify(object):
    if object != TBD and not isinstance(object, (Column, Qname, RawSQL)):
        raise ValueError(
            "Only instances of str, Column, Qname, TBD, and RawSQL are valid."
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


def listify(o: t.Any):
    return o if isinstance(o, list) else [o]


def joinmap(o, f: t.Callable = ds_name, seperator: str = ", ") -> str:
    """ Returns a seperated list of f(i) for i in o. """
    return seperator.join([str(f(o)) for o in listify(o)])


@functools.singledispatch
def table_ident(object: str) -> Qname:
    return tablify(object)  # type: ignore


@table_ident.register
def _(object: Table) -> Qname:  # type: ignore
    return object.identity


@table_ident.register
def _(object: Qname) -> Qname:
    return object


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
    if not isinstance((a_str := lo(ds_sql(a))), str) or not isinstance(
        (b_str := lo(ds_sql(b))), str
    ):
        match = False
    elif re.sub(r"\s+", " ", a_str).strip() == re.sub(r"\s+", " ", b_str).strip():
        match = True
    return match


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
