from enum import Enum
from typing import Any, List, Optional


class ColumnType:
    def __init__(
        self, python_type: type, sql_type: str, precision: Optional[str] = None
    ):
        self.python_type = python_type
        self._sql_type = sql_type
        self.precision = precision

    @property
    def sql_type(self):
        if self.precision:
            return f"{self._sql_type}({self.precision})"
        else:
            return self._sql_type

    def __call__(self, precision: Optional[str] = None):
        return ColumnType(self.python_type, self._sql_type, precision)


class ColumnUseCase(Enum):
    CREATE_COLUMNS = 0
    SELECT_COLUMNS = 1


class SQLObject:
    def sql(self):
        raise NotImplementedError("Subclasses should implement this method")

    def mount(self, parent: "SQLObject"):
        if getattr(self, "parent", None) is not None:
            raise RuntimeError(f"Object {self} already has parent {self.parent}")
        self.parent = parent

class BaseColumn(SQLObject):
    column_name: str
    python_type: Any
    inline_constraints: str
    constraints: List["BaseConstraint"]


class BaseTable(SQLObject):
    pass


class BaseConstraint(SQLObject):
    pass


class BasePKey(BaseConstraint):
    pass


class BaseFKey(BaseConstraint):
    pass


class BaseUnique(BaseConstraint):
    pass
