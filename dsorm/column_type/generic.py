import datetime
import decimal

from dsorm.base_types import ColumnType

# Numeric types
INTEGER = ColumnType(int, "INT")
FLOAT = ColumnType(float, "FLOAT")
DECIMAL = ColumnType(decimal.Decimal, "DECIMAL")

# Text types
VARCHAR = ColumnType(
    str, "VARCHAR", precision=255
)  # Assuming a default length of 255 characters

# Date and time types
DATE = ColumnType(datetime.date, "DATE")
TIME = ColumnType(datetime.time, "TIME")
TIMESTAMP = ColumnType(datetime.datetime, "TIMESTAMP")

# Binary types
BLOB = ColumnType(bytes, "BLOB")


type_dict = {
    column_type.python_type: column_type
    for column_type in [
        INTEGER,
        FLOAT,
        DECIMAL,
        VARCHAR,
        DATE,
        TIME,
        TIMESTAMP,
        BLOB,
    ]
}
