from dsorm.column_type.column_type import ColumnType

from dsorm.column_type.generic import type_dict as generic_types

# JSON types
JSON_LIST = ColumnType(list, "JSON")  # For Python lists and dictionaries
JSON_DICT = ColumnType(dict, "JSON")  # For Python lists and dictionaries


type_dict = {
    **generic_types,
    **{column_type.python_type: column_type for column_type in [JSON_DICT, JSON_LIST]},
}
