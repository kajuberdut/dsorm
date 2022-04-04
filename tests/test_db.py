from unittest import main

from dsorm import Database, Pragma

from .db_mixin import DB


class TestDB(DB):
    db_path = ":memory:"

    def test_db_setter(self):
        db = Database(self.db_path)
        db.default_db = self.db_path
        db.default_db = None

    def test_memory_method(self):
        m1 = Database.memory()
        m2 = Database(db_path=":memory:")
        self.assertEqual(m1.c, m2.c)

    def test_initialize(self):
        p = Pragma.from_dict({"foreign_keys": 1})
        self.assertEqual(p[0].sql(), "PRAGMA foreign_keys=1")

    def test_pragma(self):
        pragma = Pragma.from_dict(
            {
                "foreign_keys": 1,
                "temp_store": 2,
            }
        )
        for p in pragma:
            self.assertIn("=", p.sql())

    def test_db_close(self):
        db = Database(self.db_path)
        db.close()

    def test_execute(self):
        result = Database(self.db_path).execute("select 1 as stuff")
        self.assertEqual(result[0]["stuff"], 1)


if __name__ == "__main__":
    main()  # pragma: no cover
