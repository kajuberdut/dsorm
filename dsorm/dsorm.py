"""
D.S.O: Darned Simple ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""
import dataclasses
import functools
import sqlite3
from sqlite3.dbapi2 import OperationalError
import typing as t
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from inspect import getattr_static, signature


# SECTION 1: Database
class Database:

    connection_pool: t.Dict[str, sqlite3.Connection] = dict()
    _default_db: t.Optional[str] = None
    information_schema: t.Dict = defaultdict(dict)
    pre_connect_hook: t.Callable = lambda x: x
    post_connect_hook: t.Callable = lambda x: x

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
        self._c = self.connection_pool.get(self.db_path)
        if self._c is None:
            try:
                self.connection_pool[self.db_path] = sqlite3.connect(self.db_path)
                self._c = self.connection_pool[self.db_path]
                self._c.row_factory = self.dict_factory
            except TypeError as e:
                print(f"Connection failed for path: {self.db_path}")
                raise
        self.post_connect_hook()

    @property
    def c(self) -> sqlite3.Connection:
        if self._c is None:
            self.connect()
        return self._c

    def close(self):
        self.c.close()
        self._c = None
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
        command: str,
        parameters: t.Union[t.Tuple, t.Dict] = None,
        commit: bool = True,
    ):
        """ Execute a sql command with optional parameters """
        with self.cursor() as cur:
            try:
                cur.execute(
                    *[i for i in [ds_sql(command), parameters] if i is not None]
                )
            except (OperationalError, ValueError) as e:
                print(f"Syntax error in: {ds_sql(command)}\n\nError: {str(e)}")
                raise
            if commit:
                self.commit()
            return cur.fetchall()


# SECTION 2: Type handlers
class TypeHandlerABC(metaclass=ABCMeta):
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
        pass

    @staticmethod
    @abstractmethod
    def s2p(value) -> t.Any:
        """ This method should handle converting a SQLite datatype to a Python datatype. """
        pass


class TypeHandler(TypeHandlerABC):
    @classmethod
    def register(cls):
        if isinstance(getattr_static(cls, "p2s"), staticmethod) and isinstance(
            getattr_static(cls, "s2p"), staticmethod
        ):
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


class TypeMaster:
    type_handlers = {
        str: StrHandler,
        int: IntHandler,
        float: FloatHandler,
        datetime: DateHandler,
    }

    @classmethod
    def register(cls, handler: TypeHandler) -> None:
        cls.type_handlers[handler.python_type] = handler

    @classmethod
    def get(cls, python_type: type) -> t.Callable:
        return cls.type_handlers.get(python_type, lambda x: x)

    @classmethod
    def cast(cls, o) -> str:
        return cls.get(type(o)).p2s(o)

    def __getitem__(self, key: type) -> TypeHandler:
        return self.type_handlers[key]


# SECTION 3: Utility functions
LINE = "\n"
TAB = "\t"
ComparisonOperator = t.Literal["=", ">", "<", "!=", "<>", ">=", "<="]
ID_COLUMN = {"python_type": int, "pkey": True}


def resolve(o: t.Any, attrs: t.List = []):
    for a in attrs:
        try:
            return getattr(o, a)()
        except AttributeError:
            pass
        except TypeError as e:
            if "is not callable" in str(e):
                pass
    return o


ds_name = functools.partial(resolve, attrs=["name"])
ds_qname = functools.partial(resolve, attrs=["identity"])
ds_sql = functools.partial(resolve, attrs=["sql"])


def ds_where(where: t.Union["Where", t.Dict]) -> "Where":
    return where if isinstance(where, Where) else Where(where)


def listify(o: t.Any):
    return o if isinstance(o, list) else [o]


def joinmap(o, f: t.Callable = ds_name, seperator: str = ", ") -> str:
    """ Returns a seperated list of f(i) for i in o. """
    return seperator.join([str(f(o)) for o in listify(o)])


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


# SECTION 4: SQL Component Classes
class TBD:
    pass


@dataclasses.dataclass
class DBObject:
    db_path: str = None
    _db: "Database" = dataclasses.field(repr=False, default=None)

    @property
    def db(self):
        if self._db is None:
            self._db = Database(db_path=self.db_path)
        return self._db

    @db.setter
    def db(self, db):
        self._db = db

    def execute(self) -> t.Optional[t.List]:
        return self.db.execute(self)


@dataclasses.dataclass
class Registered(DBObject):
    name: str = None

    def __post_init__(self):
        if self.name is None:
            raise ValueError("name must be set to register object")
        self.db.information_schema[type(self).__name__][self.name] = self


class SQL(metaclass=ABCMeta):
    @abstractmethod
    def sql(self):
        pass


@dataclasses.dataclass
class Qname(SQL):
    parts: t.List = dataclasses.field(default_factory=list)

    def __add__(self, added):
        self.parts.extend(added.parts)
        return self

    def sql(self):
        return ".".join([f"[{i}]" for i in self.parts if i is not None])

    @property
    def name(self):
        return self.parts[-1]


@dataclasses.dataclass
class PragmaBase:
    name: str = None
    value: str = None


@dataclasses.dataclass
class Pragma(PragmaBase, Registered, SQL):
    @classmethod
    def from_dict(cls, d: t.Dict) -> t.List["Pragma"]:
        return [cls(name=k, value=v) for k, v in d.items()]

    def sql(self):
        return f"PRAGMA {self.name}={self.value}"


@dataclasses.dataclass
class Statement(SQL):
    components: t.Dict = dataclasses.field(default_factory=dict)

    @property
    def component_seperator(self):
        return LINE

    def sql(self) -> str:
        for i in self.Order:
            if self.components.get(i) is None:
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

    def __getitem__(self, key):
        if isinstance(key, int):
            key = self.Order(key)
        return self.components[key]

    def __setitem__(self, key, value):
        if isinstance(key, int):
            key = self.Order(key)
        if isinstance(key, str):
            key = self.Order[key]
        if not isinstance(key, self.Order):
            raise ValueError(f"Keys must be {self.__class__.__name__}.Order")
        self.components[key] = value

    class Order(Enum):
        BEGINNING = 1
        MIDDLE = 2
        END = 3


@dataclasses.dataclass
class TableObjectBase:
    table: "Table"

    def from_sql(self):
        self["FROM"] = f"FROM {self.table.identity.sql()}"


@dataclasses.dataclass
class WhereObjectBase:
    where: "Where" = dataclasses.field(default_factory=dict)

    def where_sql(self):
        self["WHERE"] = self.where if hasattr(self.where, "sql") else Where(self.where)


@dataclasses.dataclass
class Insert(DBObject, Statement, TableObjectBase):
    data: t.Union[t.Dict, t.List] = dataclasses.field(default_factory=dict)
    _prepared_data: t.Optional[t.Dict] = None
    replace: bool = False
    column: t.List = dataclasses.field(default_factory=list)
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
class Select(Statement, DBObject, WhereObjectBase, TableObjectBase):
    column: t.List = None

    def __post_init__(self):
        self.key_casters = {
            c.name: TypeMaster.get(c.python_type).s2p
            for c in self.column
            if hasattr(c, "name")
        }

    def select_sql(self):
        self["SELECT"] = "SELECT"

    def column_sql(self):
        self["COLUMN"] = joinmap(
            [c.identity if hasattr(c, "identity") else c for c in self.column], ds_sql
        )

    def cast(self, column_name: str, value: t.Any) -> t.Any:
        return self.key_casters.get(column_name, lambda x: x)(value)

    def execute(self) -> t.Optional[t.List]:
        return [
            {k: self.cast(k, v) for k, v in d.items()} for d in self.db.execute(self)
        ]

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
class Delete(Statement, DBObject, WhereObjectBase, TableObjectBase):
    def delete_sql(self):
        self["DELETE"] = "DELETE"

    class Order(Enum):
        DELETE = 1
        FROM = 2
        WHERE = 3


@dataclasses.dataclass
class ColumnBase:
    name: str = None


@dataclasses.dataclass
class Column(Statement, DBObject, ColumnBase):
    python_type: type = str
    unique: bool = False
    nullable: bool = True
    pkey: bool = False
    default: t.Any = dataclasses.field(hash=False, default=None)
    table: t.Optional["Table"] = None

    @classmethod
    def from_tuple(cls, t: t.Tuple) -> "Column":
        name, detail = t
        if isinstance(detail, type):
            return cls(name=name, python_type=detail)
        return cls(**{"name": name, **detail})

    @classmethod
    def id(cls):
        return cls.from_tuple(("id", ID_COLUMN))

    @property
    def component_seperator(self):
        return " "

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
    def identity(self):
        return (self.table.identity if self.table else Qname()) + Qname([self.name])

    def name_sql(self):
        return self.name

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

    class Order(Enum):
        NAME = 1
        TYPE = 2
        NOTNULL = 3
        UNIQUE = 4
        PRIMARYKEY = 5
        DEFAULT = 6

    def __repr__(self):
        return self.identity.sql()


@dataclasses.dataclass
class ForeignKeyBase:
    column: t.List[Column]
    reference: t.List[Column]


@dataclasses.dataclass
class ForeignKey(Statement, ForeignKeyBase):
    def foreignkey_sql(self):
        self["FOREIGNKEY"] = f"FOREIGN KEY ({joinmap(self.column)})"

    def references_sql(self):
        table_name = listify(self.reference)[0].table.identity.sql()
        self["REFERENCES"] = f"REFERENCES {table_name}({joinmap(self.reference)})"

    class Order(Enum):
        FOREIGNKEY = 1
        REFERENCES = 2


@dataclasses.dataclass
class TableBase:
    column: t.List


@dataclasses.dataclass
class Table(Statement, Registered, TableBase):
    constraints: t.List = dataclasses.field(default_factory=list)
    schema: str = None

    @classmethod
    def from_dict(cls, name: str, d: t.Dict) -> "Table":
        return cls(name=name, column=[Column.from_tuple(i) for i in d.items()])

    def __post_init__(self):
        for c in self.column:
            c.table = self
        super().__post_init__()
        self["OPARENTHESIS"] = "("
        self["CPARENTHESIS"] = ")"

    def create_sql(self):
        return f"CREATE TABLE IF NOT EXISTS {self.name}"

    def column_sql(self):
        return f"{joinmap(self.column, ds_sql)}"

    def constraint_sql(self):
        return f"{joinmap(self.constraints, ds_sql)}"

    def pkey(self) -> t.List:
        return [c for c in self.column if c.pkey]

    def fkey(self, on_column: Column = None) -> ForeignKey:
        primary = self.pkey()
        if on_column is None:
            on_column = primary
        return ForeignKey(column=on_column, table=self, reference_column=primary)

    @property
    def identity(self):
        return Qname([self.schema, self.name])

    def insert(
        self, data: t.Union[t.Dict, t.List], column: t.List = None, replace: bool = False
    ) -> Statement:
        return Insert(
            data=data,
            column=column if column else self.column,
            replace=replace,
            table=self,
        )

    def select(self, where: "Where" = None, column: t.List = None) -> Statement:
        return Select(
            where=where if where is not None else Where(),
            column=column if column else self.column,
            table=self,
        )

    def delete(self, where: t.Dict) -> Statement:
        return Delete(table=self, where=where)

    def __repr__(self):
        return self.identity.sql()

    class Order(Enum):
        CREATE = 1
        OPARENTHESIS = 2
        COLUMN = 3
        CONSTRAINTS = 4
        CPARENTHESIS = 5


@dataclasses.dataclass
class Where(SQL):
    where: dict = dataclasses.field(default_factory=dict)
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
                extras.append(f"{LINE + k} ({v.sql() + LINE})")
            else:
                if isinstance(v, (str, int, float)):
                    v = self.get_comparison(column=k, target=v)
                if hasattr(v, "column") and v.column == TBD:
                    v.column = Column(name=k)
                clause_list.append(v)
        return f"""{self.keyword} {joinmap(clause_list, ds_sql, seperator=LINE + TAB + "AND")}{joinmap(extras, seperator=LINE)}"""

    @dataclasses.dataclass
    class Comparison:
        column: Statement
        target: Statement
        operator: ComparisonOperator

        def sql(self):
            if self.column == TBD:
                raise TypeError("Column argument is required")
            return f"{ds_qname(self.column)} {self.operator} {TypeMaster.cast(self.target)}"

    @classmethod
    def get_comparison(
        cls,
        column: Statement = TBD,
        target: Statement = None,
        operator: ComparisonOperator = "=",
    ) -> "Where.Comparison":
        if target is None:
            raise TypeError("target argument is required")
        return cls.Comparison(column=column, target=target, operator=operator)

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
        column: Statement = TBD
        target: Statement = TBD
        invert: bool = False

        def sql(self):
            return f"""{ds_qname(self.column)} {"NOT" if self.invert else ""} IN ({joinmap(self.target, TypeMaster.cast)})"""

    @classmethod
    def is_in(
        cls,
        column: Statement = TBD,
        target: Statement = None,
        invert: bool = False,
    ) -> "Where.In":
        if target is None:
            raise TypeError("target argument is required")
        return cls.In(column=column, target=target, invert=invert)

    not_in = functools.partialmethod(is_in, invert=True)
