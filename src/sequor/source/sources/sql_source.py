from typing import Any, Dict
from sequor.source.source import Source
from sqlalchemy import create_engine, text

from sequor.source.sources.sql_connection import SQLConnection
from sequor.source.table_address import TableAddress

class SQLSource(Source):
    """Class representing a SQL data source"""
    def __init__(self, context: 'Context', name: str,  source_def: Dict[str, Any]):
        super().__init__(context, name, source_def)
        source_rendered_def = self.get_rendered_def()
        self.username = source_rendered_def.get('username')
        self.password = source_rendered_def.get('password')
        self.connStr = source_rendered_def.get('conn_str')
    
    def connect(self):
        return SQLConnection(self)

    def get_default_namespace_name(self):
        return "public"

    def get_qualified_name(self, table_addr: TableAddress):
        return f"{table_addr.namespace_name}.{table_addr.table_name}" if table_addr.namespace_name else table_addr.table_name

    def get_create_table_sql(self, query: str, table_addr: TableAddress) -> str:
        target_table_qualified = self.get_qualified_name(table_addr)
        query = f"CREATE TABLE {target_table_qualified} AS {query}"
        return query