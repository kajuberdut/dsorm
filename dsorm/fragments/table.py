from collections import defaultdict
from dsorm.db_objects.base_types import BaseTable


def create_table(table: BaseTable):
    return f"CREATE TABLE {table.full_table_name} " f" ({table.column_list.sql()});"


CREATE_TABLE_DICT = defaultdict(lambda: create_table)
