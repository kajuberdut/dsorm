from typing import ClassVar

from dsorm import CURRENT_DIALECT
from dsorm.fragments.constraint import FKEY_DICT, UNIQUE_DICT, pkey_constraint
from dsorm.fragments.index import DROP_INDEX_DICT, INDEX_DICT
from dsorm.fragments.table import CREATE_TABLE_DICT

FRAGMENT_DATA = {
    "pkey_constraint": pkey_constraint,
    "unique": UNIQUE_DICT,
    "fkey": FKEY_DICT,
    "create_table": CREATE_TABLE_DICT,
    "create_index": INDEX_DICT,
    "drop_index": DROP_INDEX_DICT,
}

def __getattr__(name):
    return FRAGMENT_DATA[name][CURRENT_DIALECT]