from typing import Optional

from databases import Database

from dsorm.decorators.dbclass import dbclass
from dsorm.decorators.typing import DBClass
from dsorm.dialect import SQLDialect, SQLDialectType, db_url_to_dialect, get_sqldialect
from dsorm.utility import resolve

current_dialect: SQLDialectType = get_sqldialect("sqlite")
default_schema: Optional[str] = None
db: Optional[Database] = None


def setup(db_url: str, tables: Optional[list] = None):
    global current_dialect, db

    current_dialect = db_url_to_dialect(db_url)
    db = Database(db_url)

    for table in tables:
        resolve(db.execute(str(table)))
