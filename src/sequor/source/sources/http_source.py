from typing import Any, Dict
from sequor.source.source import Source
from sqlalchemy import create_engine, text

from sequor.source.sources.sql_connection import SQLConnection
from sequor.source.table_address import TableAddress

class HTTPSource(Source):
    def __init__(self, context: 'Context', name: str,  source_def: Dict[str, Any]):
        super().__init__(context, name, source_def)
    