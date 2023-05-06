from enum import Enum


class SQLDialect(Enum):
    SQLITE = 0
    MYSQL = 1
    POSTGRESQL = 2
    DUCKDB = 3
