from unittest import TestCase, main

from dsorm import Comparison, Where
from dsorm.dsorm import columnify


class TestWhere(TestCase):
    def test_comparison(self):
        w = Where(
            {"column_name": Comparison.get_comparison(target="thingy", key="thingy")}
        )
        self.assertEqual(w.sql(), "WHERE [column_name] = :thingy")

    def test_comparison_no_target(self):
        with self.assertRaises(TypeError):
            w = Comparison.get_comparison()
            w.sql()

    def test_comparison_no_column(self):
        with self.assertRaises(TypeError):
            w = Comparison.get_comparison(target="thingy")
            w.sql()

    def test_in(self):
        w = Comparison.is_in(column="value", target=[1, 2])
        self.assertEqual(w.sql(), "value  IN (1, 2)")

    def test_in_no_target(self):
        with self.assertRaises(TypeError):
            w = Comparison.is_in()
            w.sql()

    def test_where_construction(self):
        w = Where()
        w[1] = 2
        w["this"] = "that"
        self.assertEqual(w[1], 2)
        self.assertEqual(list(w.items()), [(1, 2), ("this", "that")])

    def test_nested_where(self):

        AUTHOR_NAME = "JK Rowling"
        BOOK_NAME = "Harry Potter"
        column_a = columnify("book.name")
        column_b = columnify("author.name")
        w = Where(
            where={
                column_a: Comparison.eq(target=BOOK_NAME, key="BookName"),
                "or": Where(
                    {column_b: Comparison.eq(target=AUTHOR_NAME, key="AuthorName")}
                ),
            }
        )
        self.assertEqual(
            w.sql(),
            "WHERE [book].[name] = :BookName or ([author].[name] = :AuthorName)",
        )


if __name__ == "__main__":
    main()  # pragma: no cover
