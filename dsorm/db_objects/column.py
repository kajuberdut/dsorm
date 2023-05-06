from typing import Any, List

from dsorm import column_type
from dsorm.db_objects.constraint import Constraint


class Column:
    def __init__(
        self,
        column_name: str,
        python_type: Any,
        inline_constraints: str = None,
        constraints: List[Constraint] = None,
    ):
        self.column_name = column_name
        self.python_type = python_type
        self.inline_constraints = inline_constraints
        self.constraints = constraints or []

    def sql(self):
        inline_constraints_sql = (
            f" {self.inline_constraints}" if self.inline_constraints else ""
        )
        return f"{self.column_name} {column_type[self.python_type]}{inline_constraints_sql}"

    @classmethod
    def primary_key(cls, column_name: str):
        return cls(column_name, int, inline_constraints="NOT NULL PRIMARY KEY")


class ColumnList:
    def __init__(self, *columns: Column):
        self.columns = columns

    def sql(self):
        columns_sql = ", ".join(column.sql() for column in self.columns)

        constraints = []
        for column in self.columns:
            constraints.extend(column.constraints)

        constraints_sql = ", ".join(constraint.sql() for constraint in constraints)

        return f"{columns_sql}{', ' if constraints_sql else ''}{constraints_sql}"
