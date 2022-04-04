from unittest import TestCase

from dsorm import Column, Database, Table


class DB(TestCase):
    db_path = ":memory:"

    @property
    def db(self):
        if not hasattr(self, "_db"):
            self._db = Database(self.db_path, is_default=True)
        return self._db

    @property
    def table_setup(self):
        test_table = Table(
            db_path=":memory:",
            table_name="test",
            column=[
                Column(column_name="test_id", python_type=int, pkey=True),
                Column(column_name="stuff", unique=True, nullable=False),
            ],
        )
        self.db.execute(test_table)
        return test_table
