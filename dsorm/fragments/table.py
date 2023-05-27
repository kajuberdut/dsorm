from collections import defaultdict
from dsorm.db_objects.base_types import BaseTable


def child_list(table):
    columns_sql = ", ".join(str(column) for column in table.children)

    constraints = []
    for column in table.children:
        constraints.extend(column.constraints)

    constraints_sql = ", ".join(str(constraint) for constraint in constraints)

    return f"{columns_sql}{', ' if constraints_sql else ''}{constraints_sql}"


def create_table(table: BaseTable):
    return (
        f"CREATE TABLE {'IF NOT EXISTS ' if table.if_not_exists else ''}"
        f"{table.full_name} ({child_list(table)})"
    )


CREATE_TABLE_DICT = defaultdict(lambda: create_table)
