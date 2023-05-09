from enum import Enum
from typing import Any, List, Optional

from dsorm import fragments
from dsorm.column_type import TypeKeeper
from dsorm.db_objects.base_types import BaseColumn, BaseConstraint, BaseTable


class ColumnUseCase(Enum):
    CREATE_COLUMNS = 0
    SELECT_COLUMNS = 1


class Column(BaseColumn):
    parent: Optional[BaseTable]

    def __init__(
        self,
        column_name: str,
        python_type: Any,
        inline_constraints: str = None,
        constraints: List[BaseConstraint] = None,
    ):
        self.column_name = column_name
        self.python_type = python_type
        self.inline_constraints = inline_constraints
        self.constraints = constraints or []

    def mount(self, parent:BaseTable):
        super().mount(parent=parent)

    def sql(self):
        inline_constraints_sql = (
            f" {self.inline_constraints}" if self.inline_constraints else ""
        )
        return f"{self.column_name} {TypeKeeper[self.python_type]}{inline_constraints_sql}"

    @classmethod
    def primary_key(cls, column_name: str):
        pkey_constraint = fragments.pkey_constraint
        return cls(column_name, int, inline_constraints=pkey_constraint)


class ColumnList:
    def __init__(self, *columns: Column):
        self.columns = columns

    def mount(self, table: BaseTable):
        [column.mount(table) for column in self.columns]

    def sql(self):
        columns_sql = ", ".join(column.sql() for column in self.columns)

        constraints = []
        for column in self.columns:
            constraints.extend(column.constraints)

        constraints_sql = ", ".join(constraint.sql() for constraint in constraints)

        return f"{columns_sql}{', ' if constraints_sql else ''}{constraints_sql}"
