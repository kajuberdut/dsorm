"""
D.S.O: Darned Simple ORM
This module provides some abstractions of SQL concepts into Object Relation Mapping models.
"""
import abc
import collections
import dataclasses
import sqlite3
import typing as t

# Definitely need a better name for this type alias
ObjectTypes = t.List[t.Literal["Pragma", "Table"]]


@dataclasses.dataclass
class DSOObject(abc.ABC):
    @abc.abstractmethod
    def sql(self) -> str:
        ...


class RegisteredObject(DSOObject):
    """ Registered Objects are automatically registered in the information schema of their database."""

    db: "Database" = None

    def __post_init__(self):
        if self.db is None:
            self.db = Database
        self.db.information_schema[type(self).__name__][self.name] = self


def name(o: t.Union[DSOObject, str]) -> str:
    if isinstance(o, DSOObject):
        return o.name
    else:
        return o


def sql(o: t.Union[DSOObject, str]) -> str:
    if isinstance(o, DSOObject):
        return o.sql()
    else:
        return o


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

    def __repr__(self):
        return f"{self.table.name}.{self.name}"


@dataclasses.dataclass
class ForeignKey(RegisteredObject):
    column: t.Union[t.List, str, DSOObject]
    reference_table: "Table"
    reference_column: t.List

    def target_column_sql(self):
        if isinstance(self.column, (str, DSOObject)):
            return name(self.column)
        if isinstance(self.column, collections.abc.Iterable):
            return ",".join([name(c) for c in self.column])

    @property
    def name(self) -> str:
        return f"Fkey on {self.target_column_sql()}"

    def reference_column_sql(self):
        if isinstance(self.reference_column, (str, DSOObject)):
            return name(self.reference_column)
        if isinstance(self.reference_column, collections.abc.Iterable):
            return ",".join([name(c) for c in self.reference_column])

    def sql(self):
        return f"FOREIGN KEY ({self.target_column_sql()}) REFERENCES {name(self.reference_table)}({self.reference_column_sql()})"

    def __repr__(self):
        return f"FKEY {name(self.reference_table)}({self.reference_column_sql()})"


@dataclasses.dataclass
class Table(RegisteredObject):
    column: t.List
    name: str = None
    constraints: t.List = dataclasses.field(default_factory=list)

    def __post_init__(self):
        for c in self.column:
            c.table = self
        super().__post_init__()

    def column_sql(self):
        return ",\n".join([sql(c) for c in [*self.column, *self.constraints]])

    def sql(self):
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n {self.column_sql()})"

    def pkey(self) -> t.List[DSOObject]:
        return [c for c in self.column if c.pkey]

    def fkey(self, on_column: t.Union[str, DSOObject] = None) -> ForeignKey:
        primary = self.pkey()
        if on_column is None:
            on_column = primary
        return ForeignKey(
            column=on_column, reference_table=self, reference_column=primary
        )

    def __repr__(self):
        columns = ", ".join([name(c) for c in self.column])
        return f"{self.name}({columns})"

    def select(self, where: t.Dict = None, columns: t.List = None):
        if columns is None:
            columns = self.column
        column_sql = "\n\t, ".join([name(c) for c in columns])
        if where is not None:
            where_sql = "\n\tAND ".join([f"{k} = ?" for k in where.keys()])
        else:
            where, where_sql = dict(), None

        return (
            f"""SELECT {column_sql} \n FROM {self.name} \n {"WHERE" if where_sql else ""} {where_sql}""",
            tuple(where.values()),
        )

    def insert(self, data: t.Dict, replace: bool = False):
        k = data.keys()
        column_sql = "\n\t, ".join([name(c) for c in k])
        value_sql = "\n\t, ".join([f":{c}" for c in k])
        return (
            f"""{"REPLACE" if replace else "INSERT"} INTO {self.name} (
            {column_sql}
            ) 
        VALUES({value_sql});""",
            data,
        )

    def delete(self, where: t.Dict) -> None:
        where_sql = "\n\tAND ".join([f"{k} = ?" for k in where.keys()])
        where_values = tuple(where.values())
        return (
            f"""DELETE FROM {self.name} \n {"WHERE" if where_sql else ""} {where_sql}""",
            where_values,
        )


def dict_factory(
    cursor: sqlite3.Cursor, row: sqlite3.Row
) -> t.Dict[t.Any, t.Any]:  # pragma: no cover
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class Database:

    connection_pool: t.Dict[str, sqlite3.Connection] = dict()
    default_db: str = None
    information_schema: t.Dict = collections.defaultdict(dict)

    @classmethod
    def set_default_db(cls, db_path: str) -> None:
        """ Sets the global default database path. """
        cls.default_db = db_path

    @classmethod
    def clear_default_db(cls) -> None:
        """ Clears the global default database path. """
        cls.default_db = None

    def table(self, o: t.Union["DSOObject", str]) -> "DSOObject":
        if isinstance(o, Table):
            return o
        else:
            return self.information_schema["Table"][o]

    @property
    def conn(self) -> t.Optional[sqlite3.Connection]:
        return self.connection_pool.get(self.db_path)

    @conn.setter
    def conn(self, connection: sqlite3.Connection) -> None:
        self.connection_pool[self.db_path] = connection

    def __init__(self, db_path: str = None):
        if db_path:
            self.db_path = db_path
        elif self.default_db is None:
            raise ValueError("No default set, a db_path must be provided.")
        else:
            self.db_path = self.default_db

        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = dict_factory

    def __enter__(self) -> "Database":
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection_pool.get(self.db_path):
            self.cursor.close()

    def rollback(self):
        self.conn.rollback()

    def commit(self):
        """ Rollback the connection. """
        self.conn.commit()

    def execute(
        self,
        command: str,
        parameters: t.Union[t.Tuple, t.Dict] = None,
        auto_commit: bool = True,
    ):
        """ Execute a sql command with optional parameters """
        if parameters:
            self.cursor.execute(command, parameters)
        else:
            self.cursor.execute(command)
        if auto_commit:
            self.conn.commit()
        return self.cursor

    def executescript(self, script: str):
        """ Execute a sql command with optional parameters """
        self.cursor.executescript(script)

    def close(self):
        self.conn.close()
        del self.connection_pool[self.db_path]

    def sql(self, object_types: ObjectTypes = None) -> t.List[str]:
        """ Return the sql from one or more object types in information_schema. """
        if object_types is None:
            object_types = ["Pragma", "Table"]
        sql_set = list()
        [
            [sql_set.append(sql(o)) for o in self.information_schema[t].values()]
            for t in object_types
        ]
        return sql_set

    def query(
        self, table: t.Union["Table", str], where: t.Dict, columns: t.List = None
    ) -> t.List:
        sql, values = self.table(table).select(where=where, columns=columns)
        cur = self.conn.cursor()
        cur.execute(sql, values)
        result = cur.fetchall()
        cur.close()
        return result

    def create(self, table: t.Union["Table", str], data: t.Dict) -> None:
        c = self.conn
        sql, data = self.table(table).insert(data=data)
        cur = c.cursor()
        cur.execute(sql, data)
        result = c.commit()
        cur.close()

    def delete(self, table: t.Union["Table", str], where: t.Dict) -> None:
        c = self.conn
        sql, values = self.table(table).delete(where=where)
        cur = c.cursor()
        cur.execute(sql, values)
        result = c.commit()
        cur.close()


def init_db(db_path: str = None, object_types: ObjectTypes = None):
    """ Create basic db objects. """

    with Database(db_path=db_path) as db:
        sql = ";\n\n".join(db.sql(object_types=object_types))
        db.executescript(sql)
        db.commit()
