from typing import Any, Dict
from source.source import Source
from sqlalchemy import create_engine, text

from source.sources.sql_connection import SQLConnection
from source.table_address import TableAddress

class SQLSource(Source):
    """Class representing a SQL data source"""
    def __init__(self, name: str,  conf: Dict[str, Any]):
        """
        Initialize a SQL source with a name.
        
        Args:
            name: Name of the source
        """
        
        self.username = conf.get('username')
        self.password = conf.get('password')
        self.connStr = conf.get('conn_str')
        super().__init__(name, conf) 
    
    def connect(self):
        return SQLConnection(self)

    def get_default_namespace_name(self):
        return "public"

    def get_qualified_name(self, table_addr: TableAddress):
        return f"{table_addr.namespace_name}.{table_addr.table_name}" if table_addr.namespace_name else table_addr.table_name

    def get_create_table_sql(self, query: str, table_addr: TableAddress) -> str:
        """
        Generate a CREATE TABLE SQL statement based on the query and target table name.
        
        Args:
        """
        target_table_qualified = self.get_qualified_name(table_addr)
        query = f"CREATE TABLE {target_table_qualified} AS {query}"
        return query