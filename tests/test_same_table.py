from itertools import product
from unittest import TestCase, main

from dsorm import Column, Qname, RawSQL, Table, same_table


class TestSameTable(TestCase):

    table = Table(
        table_name="bob", column=[Column(column_name="a")], schema_name="bobsscheme"
    )
    qname = Qname(table_name="bob", schema_name="bobsScheme")
    string = "[bobsScheme].[bob]"
    raw = RawSQL(text="[bobsScheme].[Bob]")

    @property
    def items(self):
        return [self.table, self.qname, self.string, self.raw]

    def test_all_combinations(self):
        for tablelike in product(self.items, self.items):
            types = [type(t) for t in tablelike]
            with self.subTest(msg=f"test same table for: {types}", t=tablelike):
                self.assertTrue(same_table(*list(tablelike)))


if __name__ == "__main__":
    main()  # pragma: no cover
