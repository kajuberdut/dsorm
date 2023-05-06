from dsorm.fragments import Fragments
from dsorm.base_types import BaseFKey, BaseUnique, BaseColumn
from typing import Optional, List

class FKey(BaseFKey):

    parent: BaseColumn

    def __init__(self, references: BaseColumn, column: Optional[BaseColumn] = None):
        self.references = references
        self.parent = column

    def sql(self):
        return Fragments["fkey"](self.parent, self.references)


class Unique(BaseUnique):
    def __init__(self, column: Optional[BaseColumn | List[BaseColumn]] = None):
        self.column = column

    def sql(self):
        return Fragments["unique"](self.column)
