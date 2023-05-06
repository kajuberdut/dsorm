from dsorm.dbclass import dbclass
from dsorm.dblite import dblite
from dsorm.typing import DBClass
from dsorm.dialect import SQLDialect


CURRENT_DIALECT = SQLDialect.SQLITE

class Dialect:
    def __getitem__(self, key):
        global CURRENT_DIALECT
        try:
            CURRENT_DIALECT = SQLDialect[key]
        except KeyError:
            CURRENT_DIALECT = SQLDialect(key)

__getitem__ = Dialect()
