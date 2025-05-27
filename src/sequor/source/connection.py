from typing import Union
from sequor.source.model import Model
from sequor.source.row import Row
from sequor.source.source import Source
from sequor.source.table_address import TableAddress

class Connection:
    """Class representing a source connection"""
    def __init__(self, source: Source):
        self.source = source 
        self.model: Model | None = None # used by DataLoader to store schema as it opens conn per table

    def get_model(self, table_addr: TableAddress):
        raise NotImplementedError("Subclasses must implement get_model()")

    def drop_table(self, table_addr: TableAddress, only_if_exists: bool = True):
        raise NotImplementedError("Subclasses must implement drop_table_if_exists()")
    def create_table(self, table_addr: TableAddress):
        raise NotImplementedError("Subclasses must implement create_table()")
    
    def execute_update(self, query: str):
        raise NotImplementedError("Subclasses must implement execute_update()")
    
    def open_table_for_insert(self, table_addr: TableAddress, model: Union[Model, None] = None):
        raise NotImplementedError("Subclasses must implement open_table_for_insert()")
    def insert_row(self, row: Row):
        raise NotImplementedError("Subclasses must implement insert_record()")
    def close_table_for_insert(self):
        raise NotImplementedError("Subclasses must implement close_table_for_insert()")
    
    def open_table_for_read(self, table_addr: TableAddress):
        raise NotImplementedError("Subclasses must implement open_table_for_read()")
    def open_query(self, table_addr: TableAddress, query: str):
        raise NotImplementedError("Subclasses must implement open_query()")
    def next_row(self):
        raise NotImplementedError("Subclasses must implement next_row()")
    def close_query(self):
        raise NotImplementedError("Subclasses must implement close_query()")    
    def close_table_for_read(self):
        raise NotImplementedError("Subclasses must implement close_table_for_read()")
