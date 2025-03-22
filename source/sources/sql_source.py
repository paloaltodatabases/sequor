from typing import Any, Dict
from source.source import Source
from sqlalchemy import create_engine, text

from source.sources.sql_connection import SqlConnection

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
        return SqlConnection(self)

    def get_create_table_sql(self, query: str, target_table: str) -> str:
        """
        Generate a CREATE TABLE SQL statement based on the query and target table name.
        
        Args:
        """
        query = f"CREATE TABLE {target_table} AS {query}"
        return query