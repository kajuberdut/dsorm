import decimal

from dsorm.base_types import ColumnType
from dsorm.column_type.generic import type_dict as generic_types

# Numeric types
REAL = ColumnType(float, "REAL")
NUMERIC = ColumnType(decimal.Decimal, "NUMERIC")

# Text types
TEXT = ColumnType(str, "TEXT")


type_dict = {
    **generic_types,
    **{
        column_type.python_type: column_type
        for column_type in [
            REAL,
            NUMERIC,
            TEXT,
        ]
    },
}
