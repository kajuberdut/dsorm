from dsorm.fragments.constraint import PKEY_TYPE, UNIQUE_DICT, FKEY_DICT
from typing import ClassVar
from dsorm import CURRENT_DIALECT

DEFAULT_DATA = {"pkey_type": PKEY_TYPE, "unique": UNIQUE_DICT, "fkey": FKEY_DICT}


class Fragments:
    _data: ClassVar = {}

    def __init__(self):
        self._data.update(DEFAULT_DATA)

    def __getitem__(self, key):
        return self._data[key][CURRENT_DIALECT]


FRAGMENTS_PROVIDER = Fragments()

__getitem__ = FRAGMENTS_PROVIDER
