from typing import Any, List


class SQLObject(str):
    def __new__(cls, *args, length=10, **kwargs):
        instance = super().__new__(cls)
        return instance

    def __str__(self):
        raise NotImplementedError("Subclasses should implement this method")

    def mount(self, parent: "SQLObject"):
        if getattr(self, "parent", None) is not None:
            raise RuntimeError(f"Object {self} already has parent {self.parent}")
        self.parent = parent


class BaseSchema(SQLObject):
    schema_name: str


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


class BaseIndex(SQLObject):
    index_name: str
    column: List["BaseColumn"]


inheritence_dict = {
    BaseConstraint: BaseColumn,
    BaseIndex: BaseColumn,
    BaseColumn: BaseTable,
    BaseTable: BaseSchema,
}
