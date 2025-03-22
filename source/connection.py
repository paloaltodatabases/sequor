from source.source import Source

class Connection:
    """Class representing a source connection"""
    def __init__(self, source: Source):
        self.source = source 

    def drop_table_if_exists(self, table_name: str):
        raise NotImplementedError("Subclasses must implement drop_table_if_exists()")
    
    def execute_update(self, query: str):
        raise NotImplementedError("Subclasses must implement execute_update()")

    def execute_query(self, query: str):
        raise NotImplementedError("Subclasses must implement execute_query()")