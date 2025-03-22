from sqlalchemy import create_engine, text
from source.source import Source
from source.connection import Connection

class SqlConnection(Connection):
    def __init__(self, source: Source):
        super().__init__(source)
        self.open()

    def open(self):
        self.engine = create_engine(
            self.source.connStr,
            connect_args={
                'user': self.source.username,
                'password': self.source.password
            }
        )
        self.conn = self.engine.connect()

    def close(self):
        if self.conn:
            self.conn.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def drop_table_if_exists(self, table_name: str):
        self.conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    
    def execute_update(self, query: str):
        self.conn.execute(text(query))
        self.conn.commit()

    def execute_query(self, query: str):
        return self.conn.execute(text(query))
