from typing import List, Any


class SQLObject:
    def sql(self):
        raise NotImplementedError("Subclasses should implement this method")


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
