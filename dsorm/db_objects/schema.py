from dsorm.db_objects.base_types import BaseSchema
from dsorm import CURRENT_SCHEMA

class Schema(BaseSchema):

    def __init__(self, schema_name: str):
        self.schema_name = schema_name

    def use(self):
        global CURRENT_SCHEMA
        CURRENT_SCHEMA = self.schema_name

    def __str__(self):
        return self.schema_name