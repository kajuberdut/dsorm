from typing import Any, List, Optional


class _NonChild:
    pass


NonChild = _NonChild()


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

    def __getitem__(self, key):
        if getattr(self, "children", NonChild) is NonChild:
            raise ValueError(f"{self.__class__} is not subscriptable.")
        else:
            for child in self.children:
                if child.name == key:
                    return child
            raise KeyError(f'"{key}" not found in {self}.')


class BaseSchema(SQLObject):
    schema_name: str


class BaseColumn(SQLObject):
    column_name: str
    python_type: Any
    inline_constraints: str
    constraints: List["BaseConstraint"]


class BaseTable(SQLObject):
    name: str
    children: list[BaseColumn]
    schema: Optional[str]
    if_not_exists: bool


class BaseConstraint(SQLObject):
    pass


class BasePKey(BaseConstraint):
    pass


class BaseFKey(BaseConstraint):
    pass


class BaseUnique(BaseConstraint):
    pass


class BaseIndex(SQLObject):
    column: BaseColumn
    name: str
    schema: Optional[str]
    unique: bool
    where: Optional[str]
    if_not_exists: bool


inheritence_dict = {
    BaseConstraint: BaseColumn,
    BaseIndex: BaseColumn,
    BaseColumn: BaseTable,
    BaseTable: BaseSchema,
}
