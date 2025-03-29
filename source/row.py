from typing import List
from .column import Column


class Row:
    def __init__(self):
        self.columns: list[Column] = []

    def to_dict(self) -> dict:
        return {column.name: column.value for column in self.columns}

    def add_column(self, column: Column):
        self.columns.append(column)

    def get_column(self, name: str) -> Column:
        return next((column for column in self.columns if column.name == name), None)

    def remove_column(self, name: str) -> bool:
        for i, column in enumerate(self.columns):
            if column.name == name:
                self.columns.pop(i)
                return True
        return False

    def __getitem__(self, key: str):
        column = self.get_column(key)
        if column is None:
            raise KeyError(f"Column '{key}' not found")
        return column.value

    def __setitem__(self, key: str, value):
        column = self.get_column(key)
        if column is None:
            self.add_column(Column(key, value))
        else:
            column.value = value
