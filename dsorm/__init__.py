from typing import Optional

from dsorm.decorators.dbclass import dbclass
from dsorm.decorators.dblite import dblite
from dsorm.decorators.typing import DBClass
from dsorm.dialect import SQLDialectType, get_sqldialect
from dsorm.utility import sqlite_execute, sqlite_fetch_all

CURRENT_DIALECT: SQLDialectType = get_sqldialect("sqlite")
DEFAULT_DATABASE = None
CURRENT_SCHEMA: Optional[str] = None


def setup(default_db, dialect: SQLDialectType):
    global CURRENT_DIALECT

    CURRENT_DIALECT = get_sqldialect(dialect)
