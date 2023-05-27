from typing import Optional

from dsorm import fragments
from dsorm.db_objects.base_types import BaseColumn, BaseIndex


class Index(BaseIndex):
    """TODO: Does not support multi-column indexes."""
    def __init__(
        self,
        column: BaseColumn,
        index_name: Optional[str] = None,
        schema: Optional[str] = None,
        unique: bool = False,
        where: Optional[str] = None,
        if_not_exists: bool = True
    ):
        self.column = column
        self.name = index_name or f"idx_{column.parent.name}_{column.name}"
        self.schema = schema
        self.unique = unique
        self.where = where
        self.if_not_exists = if_not_exists

    def create(self):
        return fragments.create_index(self)

    def drop(self):
        return fragments.drop_index(self)

    __str__ = create
