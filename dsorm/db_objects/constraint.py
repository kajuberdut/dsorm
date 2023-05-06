
class Constraint:
    def sql(self):
        raise NotImplementedError("Subclasses should implement this method")


class FKey(Constraint):
    def __init__(self, column_name: str, references: str):
        self.column_name = column_name
        self.references = references

    def sql(self):
        return f"FOREIGN KEY ({self.column_name}) REFERENCES {self.references}"


class Unique(Constraint):
    def __init__(self, column_name: str):
        self.column_name = column_name

    def sql(self):
        return f"UNIQUE ({self.column_name})"
