from dsorm import CURRENT_DIALECT
from dsorm.column_type.mysql import type_dict as mysql_types
from dsorm.column_type.postgres import type_dict as postgres_types
from dsorm.column_type.sqlite import type_dict as sqlite_types
from dsorm.dialect import SQLDialect

DIALECT_TYPES = {
    SQLDialect.SQLITE: sqlite_types,
    SQLDialect.MYSQL: mysql_types,
    SQLDialect.POSTGRESQL: postgres_types,
}


class _TypeKeeper:
    def __getitem__(self, name):
        return DIALECT_TYPES[CURRENT_DIALECT][name]

TypeKeeper = _TypeKeeper()