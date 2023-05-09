from collections import defaultdict

from dsorm.db_objects.base_types import BaseColumn


def make_index(*idx_col: BaseColumn, schema="", if_not_exists=True, where="", **kwargs):
    index_name = f"{idx_col.table.table_name}_{idx_col.column_name}_idx"
    return (
        f"CREATE INDEX {'IF NOT EXISTS' if if_not_exists else ''}"
        " "
        f"{schema}{'.' if schema else ''}{index_name} ON {idx_col.table.table_name}({idx_col.column_name})"
        f"{' ' if where else ''}{where}"
    )


INDEX_DICT = defaultdict(lambda: make_index)


def make_drop_index(idx_name, schema="", if_exists=True, **kwargs):
    return (
        f"DROP INDEX {'IF EXISTS'  if if_exists else ''}"
        " "
        f"{schema}{'.' if schema else ''}{idx_name}"
    )


DROP_INDEX_DICT = defaultdict(lambda: make_drop_index)
