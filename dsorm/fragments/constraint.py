from collections import defaultdict
from typing import Iterable

from dsorm.base_types import BaseColumn
from dsorm.dialect import SQLDialect

PKEY_TYPE = defaultdict(
    "NOT NULL PRIMARY KEY",
    {
        SQLDialect.POSTGRESQL: "SERIAL PRIMARY KEY",
        SQLDialect.MYSQL: "INT AUTO_INCREMENT PRIMARY KEY",
    },
)


def make_unique_fragment(col: BaseColumn | Iterable[BaseColumn]):
    if isinstance(col, BaseColumn):
        unique_cols = col.column_name
    else:
        unique_cols = ", ".join([c.column_name for c in col])
    return f"UNIQUE ({unique_cols})"


UNIQUE_DICT = defaultdict(make_unique_fragment)


def make_fkey(col: BaseColumn, ref_col: BaseColumn):
    return (
        f"FOREIGN KEY ({col.column_name}) REFERENCES "
        f"{ref_col.table.table_name}({ref_col.column_name})"
    )


FKEY_DICT = defaultdict(make_fkey)
