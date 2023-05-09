from dsorm.decorators.dbclass import dbclass
from dsorm.decorators.dblite import dblite
from dsorm.decorators.typing import DBClass
from dsorm.dialect import SQLDialect


CURRENT_DIALECT = SQLDialect.SQLITE
CURRENT_SCHEMA = None


class Dialect:
    def __getitem__(self, key):
        global CURRENT_DIALECT
        try:
            CURRENT_DIALECT = SQLDialect[key]
        except KeyError:
            CURRENT_DIALECT = SQLDialect(key)


__getitem__ = Dialect()
