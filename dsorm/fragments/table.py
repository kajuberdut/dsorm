from collections import defaultdict
from dsorm.base_types import BaseTable


def create_table(table: BaseTable):
    return f"CREATE TABLE {table.full_table_name} "f" ({table.column_list.sql()});"


CREATE_TABLE_DICT = defaultdict(create_table)
