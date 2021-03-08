from dsorm import Qname, Table, Column, RawSQL, same_table
from itertools import product


def test_all_combinations():
    t = Table(table_name="bob", column=[Column(column_name="a")], schema_name="bobsscheme")
    q = Qname(table_name="bob", schema_name="bobsScheme")
    s = "[bobsScheme].[bob]"
    r = RawSQL(text="[bobsScheme].[Bob]")
    items = [t, q, s, r]
    for t in product(items, items):
        assert same_table(*list(t))
