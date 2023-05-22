from typing import List, Optional

from dsorm.db_objects.base_types import BaseColumn, BaseFKey, BaseUnique
from dsorm import fragments


class FKey(BaseFKey):
    parent: BaseColumn

    def __init__(self, references: BaseColumn, column: Optional[BaseColumn] = None):
        self.references = references
        self.parent = column

    def __str__(self):
        return fragments.fkey(self.parent, self.references)


class Unique(BaseUnique):
    parent: BaseColumn

    def __init__(self, column: Optional[BaseColumn | List[BaseColumn]] = None):
        self.column = column

    def __str__(self):
        return fragments.unique(self.column)
