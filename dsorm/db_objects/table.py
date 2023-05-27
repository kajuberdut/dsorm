from typing import Type

from dsorm import default_schema, fragments
from dsorm.db_objects.base_types import BaseColumn, BaseTable


class Table(BaseTable):
    def __init__(
        self,
        name,
        *children: Type[BaseColumn],
        schema=None,
        if_not_exists: bool = True,
    ):
        self.name = name
        self.schema = schema or default_schema
        self.children = children
        self.if_not_exists = if_not_exists
        [column.mount(self) for column in self.children]

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name

    def __str__(self):
        return fragments.create_table(self)
