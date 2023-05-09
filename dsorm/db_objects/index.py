from dsorm import fragments
from dsorm.db_objects.base_types import BaseIndex, BaseColumn


class Index(BaseIndex):
    def __init__(self, index_name, *column):
        self.index_name = index_name
        self.column = column

    def create_sql(self):
        if not isinstance(self.column, BaseColumn):
            schema = self.column[0].parent.parent
        return fragments.create_index()

    def drop_sql(self):
        return fragments.drop_index()
