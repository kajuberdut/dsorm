from typing import ClassVar

from dsorm import CURRENT_DIALECT
from dsorm.fragments.constraint import FKEY_DICT, UNIQUE_DICT, pkey_constraint
from dsorm.fragments.table import CREATE_TABLE_DICT

DEFAULT_DATA = {
    "pkey_constraint": pkey_constraint,
    "unique": UNIQUE_DICT,
    "fkey": FKEY_DICT,
    "create_table": CREATE_TABLE_DICT,
}


class Fragments:
    _data: ClassVar = {}

    def __init__(self):
        self._data.update(DEFAULT_DATA)

    def __getitem__(self, key):
        return self._data[key][CURRENT_DIALECT]


FRAGMENTS_PROVIDER = Fragments()

__getitem__ = FRAGMENTS_PROVIDER
