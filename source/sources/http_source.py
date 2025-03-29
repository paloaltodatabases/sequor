from typing import Any, Dict
from source.source import Source
from sqlalchemy import create_engine, text

from source.sources.sql_connection import SQLConnection
from source.table_address import TableAddress

class HTTPSource(Source):
    """Class representing a SQL data source"""
    def __init__(self, name: str,  conf: Dict[str, Any]):
        """
        Initialize a SQL source with a name.
        
        Args:
            name: Name of the source
        """
        
        # self.username = conf.get('username')
        # self.password = conf.get('password')
        # self.connStr = conf.get('conn_str')
        super().__init__(name, conf) 
    