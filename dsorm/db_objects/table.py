from typing import List

from dsorm.db_objects.column import Column, ColumnList
from dsorm.db_objects.base_types import BaseTable
from dsorm import fragments, CURRENT_SCHEMA

class Table(BaseTable):
    def __init__(
        self,
        table_name,
        *columns,
        column_list: ColumnList | List[Column] | None = None,
        schema=None,
    ):
        self.table_name = table_name
        self.schema = schema or CURRENT_SCHEMA

        if not isinstance(column_list, ColumnList):
            if column_list:
                if isinstance(column_list, list):
                    self.column_list = ColumnList(*column_list)
            else:
                self.column_list = ColumnList(*columns)
        else:
            self.column_list = column_list

        self.column_list.mount(self)

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def sql(self):
        return fragments.create_table(self)


if __name__ == "__main__":

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

    from dsorm.db_objects.constraint import FKey


    id_column = Column.primary_key("id")
    name_column = Column("name", str, "NOT NULL")
    age_column = Column("age", int)

    table = Table("users", id_column, name_column, age_column, schema="public")
    print(table.sql())

    user_id_column = Column("user_id", int, constraints=FKey(references=id_column))
    table2 = Table("user_profiles", user_id_column, schema="public")
    print(table2.sql())
