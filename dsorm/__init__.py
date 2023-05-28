import asyncio
from typing import Optional

from databases import Database

from dsorm.db_url_helpers import db_url_to_dialect
from dsorm.decorators.dbclass import dbclass
from dsorm.decorators.typing import DBClass
from dsorm.dialect import SQLDialect, SQLDialectType, get_sqldialect
from dsorm.utility import resolve

current_dialect: SQLDialectType = get_sqldialect("sqlite")
default_schema: Optional[str] = None
db: Optional[Database] = None


async def setup(db_url: str, create_first: Optional[list] = None):
    global current_dialect, db

    current_dialect = db_url_to_dialect(db_url)
    db = Database(db_url)

    await db.connect()

    try:

        for creatable in create_first:
            resolve(db.execute(str(creatable)))

        yield db

    finally:
        await db.disconnect()
