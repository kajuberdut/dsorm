from typing import Type

from dsorm import default_schema, fragments
from dsorm.db_objects.base_types import BaseColumn, BaseTable


class Table(BaseTable):
    def __init__(
        self,
        table_name,
        *children: Type[BaseColumn],
        schema=None,
    ):
        self.table_name = table_name
        self.schema = schema or default_schema
        self.children = children
        [column.mount(self) for column in self.children]

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def __str__(self):
        return fragments.create_table(self)

    def __getitem__(self, key):
        for column in self.children:
            if column.column_name == key:
                return column
        raise KeyError(f'Column "{key}" not found in the table.')
