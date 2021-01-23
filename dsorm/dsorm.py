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
from datetime import datetime
from enum import Enum
from inspect import signature

# SECTION 2: Custom Types, abc classes and base classes


class Special(Enum):
    Null = 1
    NotNull = 2
    TBD = 3


@dataclasses.dataclass
class TypeHandler:
    sql_type: str
    python_type: type

    @staticmethod
    def p2s(value) -> t.Union[int, float, str, bytes]:
        """ This method should return a value that would be valid "as is" in a sql statement. """
        return value

    @staticmethod
    def s2p(value) -> t.Any:
        """ This method should handle converting a SQLite datatype to a Python datatype. """
        return value


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
        """ surround with single quotes to embed string literals in sql """
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
        return cls.type_handlers.get(python_type, no_cast)

    def __getitem__(self, key: type) -> TypeHandler:
        return self.type_handlers[key]


class TableCaster:
    def __init__(self, table: "Table"):
        self.key_casters = {
            c.name: TypeMaster.get(c.python_type).s2p for c in table.column
        }

    def cast(self, column_name: str, value: t.Any) -> t.Any:
        return self.key_casters.get(column_name, no_cast)(value)

    def cast_values(self, values: t.Union[t.List, t.Dict]) -> t.List:
        return [
            {k: self.cast(k, v) for k, v in d.items()}
            for d in (values if isinstance(values, list) else [values])
        ]


class DSObject:
    ...


SQLFragment = t.Union[DSObject, str, int, float]
Fragments = t.Union[Iterable, SQLFragment]
ComparisonOperator = t.Literal["=", ">", "<", "!=", "<>", ">=", "<="]


class RegisteredObject(DSObject):
    """ Registered Objects are automatically registered in the information schema of their database."""

    db: "Database" = None

    def __post_init__(self):
        self.register()

    def register(self):
        if self.db is None:
            self.db = Database
        self.db.information_schema[type(self).__name__][self.name] = self


# SECTION 3: Utility functions
LINE = "\n"
TAB = "\t"


def no_cast(x) -> t.Any:
    return x


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


def ds_where(where: t.Union["Where", t.Dict]) -> "Where":
    if isinstance(where, Where):
        return where
    else:
        return Where(where)


def ds_quote(o: t.Any) -> t.Union[str, int, float]:
    if isinstance(o, DSObject):
        return o.cast()
    else:
        return TypeMaster.get(type(o)).p2s(o)


def joinmap(o: Fragments, f: t.Callable = ds_name, seperator: str = ", ") -> str:
    """ Returns a comma seperated list of f(i) for i in o. """
    if isinstance(o, Iterable) and not isinstance(o, str):
        try:
            return seperator.join(map(f, o))
        except TypeError:
            return seperator.join([str(o) for o in map(f, o)])
    else:
        return f(o)


def do_nothing(*args, **kwargs):
    pass


def pre_connect(run_once=True):  # pragma: no cover
    def pre_wrapper(func):
        @functools.wraps(func)
        def pre(*args):
            func(args[0])
            if run_once:
                Database.pre_connect_hook = do_nothing

        Database.pre_connect_hook = pre

    return pre_wrapper


def post_connect(run_once=True):  # pragma: no cover
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
class Statement(DSObject):
    """An object representing a sql statement and optional values."""

    components: t.Dict[Enum, str] = dataclasses.field(default_factory=dict)
    _db: "Database" = None

    class Order(Enum):
        CTE = 1
        UPDATE = 2
        SET = 3
        INSERT = 4
        INSERT_COLUMNS = 5
        SELECT = 6
        SELECT_COLUMNS = 7
        DELETE = 8
        FROM = 9
        JOIN = 10
        VALUES = 11
        WHERE = 12
        GROUP = 13
        HAVING = 14
        ORDER = 15
        LIMIT = 16
        OFFSET = 17

    def sql(self) -> str:
        return "\n".join(
            [
                ds_sql(self.components[clause])
                for clause in self.Order
                if clause in self.components
            ]
        )


@dataclasses.dataclass
class Where(DSObject):
    where: t.Dict
    seperator: str = LINE + TAB + "AND"
    keyword: str = "WHERE"

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
                if hasattr(v, "column") and v.column == Special.TBD:
                    v.column = Column(name=k)
                clause_list.append(v)
        return f"""{self.keyword} {joinmap(clause_list, ds_sql, seperator=self.seperator)}{joinmap(extras, seperator=LINE)}"""

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
    like = functools.partialmethod(get_comparison, operator="LIKE")

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
    python_type: type = str
    type_handler: TypeHandler = None
    unique: bool = False
    nullable: bool = True
    pkey: bool = False
    db_path: str = None
    default: t.Any = None
    _table: "Table" = None

    @property
    def db(self):
        return Database(db_path=self.db_path)

    @property
    def table(self) -> "Table":
        return self._table

    @table.setter
    def table(self, table: "Table") -> None:
        self._table = table

    @property
    def default_sig(self):
        try:
            return signature(self.default)
        except TypeError as e:
            if "is not a callable object" in str(e):
                return None
            else:  # pragma: no cover
                raise e

    def default_sql(self):
        if self.default is None or self.default_sig:
            return ""
        if not callable(self.default):
            return f"DEFAULT {ds_quote(self.default)}"

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

    def sql(self):
        blocks = [
            self.name,
            TypeMaster.get(self.python_type).sql_type,
            self.default_sql(),
        ]
        if not self.nullable:
            blocks.append("NOT NULL")
        if self.unique:
            blocks.append("UNIQUE")
        if self.pkey:
            blocks.append("PRIMARY KEY")
        return " ".join(blocks)


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
    _caster: TableCaster = None

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
    def caster(self) -> TableCaster:
        if self._caster is None:
            self._caster = TableCaster(self)
        return self._caster

    @property
    def identifier(self):
        if self.schema:
            return self.schema + "." + self.name
        else:
            return self.name

    def data_prep(self, data: t.Dict) -> t.Dict:
        """Returns a dictionary ready for use in values statment.
        Column order ensures multiple value rows have the same order.
        Adds default values for missing items where column.default_sig.
        Applies quoting rules to values to make them SQL ready
        """
        result = dict()
        for c in self.column:
            value = data.get(c.name)
            if value is None:
                sig = c.default_sig
                if sig is not None:
                    if sig.parameters.get("data"):
                        value = c.default(data=data)
                    else:
                        value = c.default()
            if value is not None:
                result[c.name] = ds_quote(value)
        return result

    def header_values(self, data: t.Union[t.Dict, t.List] = None) -> t.Tuple[str, str]:
        if data is None:
            return "", "DEFAULT VALUES"
        data_list = [
            self.data_prep(d) for d in (data if isinstance(data, list) else [data])
        ]
        header = f"({', '.join(data_list[0].keys())})"
        values = (
            f"""VALUES {", ".join([f"({', '.join(d.values())})" for d in data_list])}"""
        )
        return header, values

    def insert(self, data: t.Dict, replace: bool = False) -> Statement:
        s = Statement()
        s.components[
            Statement.Order.INSERT
        ] = f"{'REPLACE' if replace else 'INSERT'} INTO {self.identifier}"
        (
            s.components[Statement.Order.INSERT_COLUMNS],
            s.components[Statement.Order.VALUES],
        ) = self.header_values(data)
        return s

    def select(self, where: t.Dict = None, columns: t.List = None) -> Statement:
        return Statement(
            components={
                Statement.Order.SELECT: "SELECT ",
                Statement.Order.SELECT_COLUMNS: joinmap(
                    columns if columns else self.column, ds_qname
                ),
                Statement.Order.FROM: f"FROM {self.identifier}",
                Statement.Order.WHERE: ds_where(where),
            }
        )

    def delete(self, where: t.Dict) -> Statement:
        return Statement(
            components={
                Statement.Order.DELETE: "DELETE",
                Statement.Order.FROM: f"FROM {self.identifier}",
                Statement.Order.WHERE: ds_where(where),
            }
        )

    def __repr__(self):
        return f"{self.identifier}({joinmap(self.column)})"


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
        self,
        table: t.Union["Table", str],
        where: t.Dict = None,
        columns: t.List = None,
        cast_values: bool = True,
    ) -> t.List:
        if isinstance(table, str):
            table = self.table(table)
        with Cursor(_db=self) as cur:
            sql = table.select(where=where, columns=columns)
            result = cur.execute(sql)
        if cast_values:
            return table.caster.cast_values(result)
        return result

    def insert(self, table: t.Union["Table", str], data: t.Dict, replace=False) -> None:
        with Cursor(_db=self) as cur:
            stmt = self.table(table).insert(data=data, replace=replace)
            cur.execute(command=stmt)

    def delete(self, table: t.Union["Table", str], where: t.Dict) -> None:
        with Cursor(_db=self) as cur:
            cur.execute(self.table(table).delete(where=where))

    def init_db(self):
        """ Create basic db objects. """
        sql_set = list()
        [
            [sql_set.append(o) for o in self.information_schema[t].values()]
            for t in ["Pragma", "Table"]
        ]
        script = joinmap(sql_set, ds_sql, seperator=";" + LINE + LINE)
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
        if isinstance(command, Statement):
            command = ds_sql(command)
        if parameters:
            self._cursor.execute(command, parameters)
        else:
            self._cursor.execute(command)
        if commit:
            self.commit()
        return self._cursor.fetchall()
