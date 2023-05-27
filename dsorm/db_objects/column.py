from typing import Any, List, Optional

from dsorm import fragments
from dsorm.column_type import TypeKeeper
from dsorm.db_objects.base_types import BaseColumn, BaseConstraint, BaseTable


class Column(BaseColumn):
    parent: Optional[BaseTable]

    def __init__(
        self,
        column_name: str,
        python_type: Any = str,
        inline_constraints: str = "NOT NULL",
        constraints: List[BaseConstraint] = None,
    ):
        self.name = column_name
        self.python_type = python_type
        self.inline_constraints = inline_constraints
        if constraints is None:
            self.constraints = []
        elif isinstance(constraints, list):
            self.constraints = constraints
        else:
            self.constraints = [constraints]

    @classmethod
    def primary_key(cls, column_name: str = "id"):
        pkey_constraint = fragments.pkey_constraint
        return cls(column_name, int, inline_constraints=pkey_constraint)

    def mount(self, parent: BaseTable):
        super().mount(parent=parent)
        [constraint.mount(self) for constraint in self.constraints]

    def __str__(self):
        inline_constraints_sql = (
            f" {self.inline_constraints}" if self.inline_constraints else ""
        )
        return (
            f"{self.name} {TypeKeeper[self.python_type]}{inline_constraints_sql}"
        )
