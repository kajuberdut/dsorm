from dsorm import default_schema
from dsorm.db_objects.base_types import BaseSchema


class Schema(BaseSchema):
    def __init__(self, schema_name: str):
        self.name = schema_name

    def use(self):
        global default_schema
        default_schema = self.name

    def create(self):
        """TODO: This only covers about 80% of dialects/use"""
        return f"CREATE SCHEMA {self.name}"

    __str__ = create
