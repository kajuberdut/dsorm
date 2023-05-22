from dsorm.db_objects.base_types import BaseTable
from dsorm import fragments, CURRENT_SCHEMA


class Table(BaseTable):
    def __init__(
        self,
        table_name,
        *children,
        schema=None,
    ):
        self.table_name = table_name
        self.schema = schema or CURRENT_SCHEMA
        self.children = children
        [column.mount(self) for column in self.children]

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def __str__(self):
        return fragments.create_table(self)
