from collections import defaultdict
from dsorm.dialect import SQLDialect

PKEY_TYPE = defaultdict(
    "NOT NULL PRIMARY KEY",
    {
        SQLDialect.POSTGRESQL: "SERIAL PRIMARY KEY",
        SQLDialect.MYSQL: "INT AUTO_INCREMENT PRIMARY KEY",
    },
)

def make_unique_fragment(col: str | list):
    if isinstance(col, str):
        unique_cols = col
    else:
        unique_cols = ", ".join(col)
    return f"UNIQUE ({unique_cols})"

UNIQUE_DICT = defaultdict(make_unique_fragment)

def make_fkey(column_name: str, references: str):
    return f"FOREIGN KEY ({column_name}) REFERENCES {references}"
