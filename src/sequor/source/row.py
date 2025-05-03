from typing import List

from sequor.core.user_error import UserError
from .column import Column


class Row:
    def __init__(self):
        self.columns: list[Column] = []

    @staticmethod
    def from_dict(data: dict) -> 'Row':
        row = Row()
        for name, value in data.items():
            row.add_column(Column(name, value))
        return row
    
    def to_dict(self) -> dict:
        return {column.name: column.value for column in self.columns}

    def add_column(self, column: Column):
        self.columns.append(column)

    def get_column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise UserError(f"Column '{name}' does not exist")

    def remove_column(self, name: str) -> bool:
        for i, column in enumerate(self.columns):
            if column.name == name:
                self.columns.pop(i)
                return True
        return False

    # ------------ dict-style access method: beginning ------------
    def __getitem__(self, key: str):
        """Access column value by name (str) or index (int)"""
        if isinstance(key, str):
            column = self.get_column(key)
            if column is None:
                raise KeyError(f"Column '{key}' does not exist")
            return column.value
        elif isinstance(key, int):
            return self.columns[key].value
        else:
            raise UserError(f"Key must be string or integer, not {type(key).__name__}")

    def get(self, key: str, default=None):
        column = self.get_column(key)
        return column.value if column.value is not None else default


    def __setitem__(self, key: str, value):
        column = self.get_column(key)
        if column is None:
            self.add_column(Column(key, value))
        else:
            column.value = value
    def __iter__(self):
        """Make Row iterable (iterates through column names)"""
        return (col.name for col in self.columns)
    
    def __len__(self):
        """Return number of columns"""
        return len(self.columns)
    
    def keys(self):
        """Return column names"""
        return (col.name for col in self.columns)
    
    def values(self):
        """Return column values"""
        return (col.value for col in self.columns)
    
    def items(self):
        """Return (name, value) pairs"""
        return ((col.name, col.value) for col in self.columns)
    
    def __contains__(self, key):
        """Support for 'in' operator"""
        if isinstance(key, str):
            return any(col.name == key for col in self.columns)
        return False
    # ------------ dict-style access method: end ------------

