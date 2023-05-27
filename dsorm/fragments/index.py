from collections import defaultdict
from typing import Optional

from dsorm.db_objects.base_types import BaseIndex


def make_index(index: BaseIndex):
    column_name, table_name = index.column.name, index.column.parent.name
    schema = index.schema
    return (
        f"CREATE {'UNIQUE ' if index.unique else ''}"
        f" INDEX {'IF NOT EXISTS ' if index.if_not_exists else ''}"
        f"{schema + '.' if schema else ''}"
        f"{'.' if index.schema else ''}{index.name} ON {table_name}({column_name})"
        f"{' ' if index.where else ''}"
    )


INDEX_DICT = defaultdict(lambda: make_index)


def make_drop_index(index: BaseIndex):
    return (
        f"DROP INDEX {'IF EXISTS'  if index.if_exists else ''}"
        " "
        f"{index.schema}{'.' if index.schema else ''}{index.name}"
    )


DROP_INDEX_DICT = defaultdict(lambda: make_drop_index)
