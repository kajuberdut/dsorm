from typing import List

from dsorm.db_objects.column import Column, ColumnList
from dsorm.db_objects.base_types import BaseTable
from dsorm import fragments, CURRENT_SCHEMA


class Table(BaseTable):
    def __init__(
        self,
        table_name,
        *columns,
        column_list: ColumnList | List[Column] | None = None,
        schema=None,
    ):
        self.table_name = table_name
        self.schema = schema or CURRENT_SCHEMA

        if not isinstance(column_list, ColumnList):
            if column_list:
                if isinstance(column_list, list):
                    self.column_list = ColumnList(*column_list)
            else:
                self.column_list = ColumnList(*columns)
        else:
            self.column_list = column_list

        self.column_list.mount(self)

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def sql(self):
        return fragments.create_table(self)
