from enum import Enum
from typing import List

from dsorm.column_type.column_type import ColumnType
from dsorm.db_objects.column import Column, ColumnList
from dsorm.dialect import SQLDialect

"""
CREATE TABLE people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    address_id INTEGER,
    FOREIGN KEY (address_id) REFERENCES address(id)
);

CREATE INDEX idx_people_email ON people(email);
"""


class SQLUseCase(Enum):
    CREATE_COLUMNS = 0
    SELECT_COLUMNS = 1


class Table:
    def __init__(
        self,
        table_name,
        *columns,
        column_list: ColumnList | List[Column] | None = None,
        schema=None,
    ):
        if column_list:
            if isinstance(column_list, list):
                column_list = ColumnList(*column_list)
        else:
            column_list = ColumnList(*columns)

        self.table_name = table_name
        self.column_list = column_list
        self.schema = schema

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def create_sql(self):
        columns_sql = self.column_list.sql()
        create_table_sql = f"CREATE TABLE {self.full_table_name} ({columns_sql});"
        return create_table_sql


if __name__ == "__main__":
    from dsorm.db_objects.constraint import FKey
    # Example usage:
    INTEGER = ColumnType(int, "INTEGER")
    TEXT = ColumnType(str, "TEXT")

    id_column = Column("id", INTEGER, inline_constraints="")
    name_column = Column("name", TEXT, "NOT NULL")
    age_column = Column("age", INTEGER)
    user_id_column = FKey("user_id", INTEGER, "users(id)")

    table = Table("users", id_column, name_column, age_column, schema="public")
    print(table.create_sql())

    column_list2 = ColumnList()
    table2 = Table("user_profiles", user_id_column, schema="public")
    print(table2.create_sql())
