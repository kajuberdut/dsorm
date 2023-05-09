import decimal

from dsorm.column_type.column_type import ColumnType
from dsorm.column_type.generic import type_dict as generic_types

# Numeric types
REAL = ColumnType(float, "REAL")
NUMERIC = ColumnType(decimal.Decimal, "NUMERIC")


# Binary types
BYTEA = ColumnType(bytes, "BYTEA")

# JSON types
JSONB_LIST = ColumnType(
    list, "JSONB"
)  # For Python lists and dictionaries, with more efficient storage
JSONB_DICT = ColumnType(
    dict, "JSONB"
)  # For Python lists and dictionaries, with more efficient storage


type_dict = {
    **generic_types,
    **{
        column_type.python_type: column_type
        for column_type in [
            REAL,
            NUMERIC,
            BYTEA,
            JSONB_LIST,
            JSONB_DICT,
        ]
    },
}
