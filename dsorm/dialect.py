from enum import Enum
from typing import TypeAlias


class SQLDialect(Enum):
    SQLITE = 0
    MYSQL = 1
    POSTGRESQL = 2
    DUCKDB = 3


SQLDialectType: TypeAlias = int | str | SQLDialect


def get_sqldialect(value: SQLDialectType) -> SQLDialect:
    if isinstance(value, int):
        return SQLDialect(value)
    elif isinstance(value, str):
        return SQLDialect[value.upper()]
    elif isinstance(value, SQLDialect):
        return value
    else:
        raise TypeError("Invalid type for SQLDialect")
