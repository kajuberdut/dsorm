from typing import Optional


class ColumnType:
    def __init__(
        self, python_type: type, sql_type: str, precision: Optional[str] = None
    ):
        self.python_type = python_type
        self._sql_type = sql_type
        self.precision = precision

    @property
    def sql_type(self):
        if self.precision:
            return f"{self._sql_type}({self.precision})"
        else:
            return self._sql_type
        
    def __str__(self):
        return self.sql_type

    def __call__(self, precision: Optional[str] = None):
        return ColumnType(self.python_type, self._sql_type, precision)
