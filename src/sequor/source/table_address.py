from typing import Any


class TableAddress:
    def __init__(self, source_name, database_name, namespace_name, table_name, model_def: Any = None, data: list = None, write_mode: str = None):
        self.source_name = source_name
        self.database_name = database_name
        self.namespace_name = namespace_name
        self.table_name = table_name
        self.model_def = model_def
        self.data = data
        self.write_mode = write_mode
    
    def clone(self):
        return TableAddress(
            source_name=self.source_name,
            database_name=self.database_name,
            namespace_name=self.namespace_name, 
            table_name=self.table_name,
            model_def=self.model_def,
            data=self.data,
            write_mode=self.write_mode
        )
