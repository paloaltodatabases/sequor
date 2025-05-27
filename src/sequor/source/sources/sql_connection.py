from typing import Union
from sqlalchemy import MetaData, Table, create_engine, text

from sequor.source.column import Column
from sequor.source.column_schema import ColumnSchema
from sequor.source.data_type import DataType
from sequor.source.model import Model
from sequor.source.row import Row
from sequor.source.source import Source
from sequor.source.connection import Connection
from sequor.source.table_address import TableAddress

class SQLConnection(Connection):
    def __init__(self, source: Source):
        super().__init__(source)
        self.open()

    def open(self):
        self.engine = create_engine(
            self.source.connStr,
            connect_args={
                'user': self.source.username,
                'password': self.source.password
            },
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

    def get_model(self, table_addr: TableAddress):
        metadata = MetaData()
        if table_addr.namespace_name is None:
            users_table = Table(table_addr.table_name, metadata, autoload_with=self.engine)
        else:
            users_table = Table(table_addr.table_name, metadata, schema=table_addr.namespace_name, autoload_with=self.engine)
        column_schemas = [ColumnSchema(c.name, DataType(c.type)) for c in users_table.columns]
        return Model.from_columns(column_schemas)

    def drop_table(self, table_addr: TableAddress, only_if_exists: bool = True):
        table_qualified_name = self.source.get_qualified_name(table_addr)
        self.conn.execute(text(f"DROP TABLE {'IF EXISTS' if only_if_exists else ''} {table_qualified_name}"))
    
    def create_table(self, table_addr: TableAddress, model: Model):
        table_qualified_name = self.source.get_qualified_name(table_addr)
        self.conn.execute(text(f"CREATE TABLE {table_qualified_name} ({', '.join([c.name + ' ' + c.type.name for c in model.columns])})"))
    
    def execute_update(self, query: str):
        self.conn.execute(text(query))
        self.conn.commit()

    def open_table_for_insert(self, table_addr: TableAddress, model: Union[Model, None] = None, autocommit: bool = False):
        self.open_table_for_insert_table_addr = table_addr
        if model is not None:
            self.open_table_for_insert_model = model
        else:
            self.open_table_for_insert_model = self.get_model(table_addr)
        table_qualified_name = self.source.get_qualified_name(table_addr)

        # build sql
        columns_sql = [c.name for c in self.open_table_for_insert_model.columns]
        placeholders_sql = [f":{c.name}" for c in self.open_table_for_insert_model.columns]
        sql = f"INSERT INTO {table_qualified_name}(" + ", ".join(columns_sql) + ") VALUES (" + ", ".join(placeholders_sql) + ")"
        
        self.open_table_for_insert_stmt = text(sql);
        self.conn.autocommit = autocommit
        self.open_table_for_insert_autocommit = autocommit

    def insert_row(self, row: Row):
        row_dict = row.to_dict()
        self.conn.execute(self.open_table_for_insert_stmt, row_dict )

    def close_table_for_insert(self):
        if not self.open_table_for_insert_autocommit:
            self.conn.commit()
        self.open_table_for_insert_stmt = None
        self.open_table_for_insert_model = None
        self.open_table_for_insert_table_addr = None

    def open_table_for_read(self, table_addr: TableAddress):
        query = f"SELECT * FROM {self.source.get_qualified_name(table_addr)}"
        self.open_query(query)

 
    def open_query(self, query_str: str):
        query = text(query_str)
        self.conn.execution_options(stream_results=True)
        self.open_table_for_read_result = self.conn.execute(query)
        # to get precision and scale use:
        # for col in self.open_table_for_read_result.cursor.description
        # name = col[0] precision = col[4] scale = col[5]
        col_schemas = []
        for col in self.open_table_for_read_result.cursor.description:
            col_name = col[0]  # Column name
            col_type = str(col[1])  # Data type (DBAPI-specific type object)
            precision = col[4]  # Precision for numeric columns
            scale = col[5]  # Scale for numeric columns (if applicable)
            col_schema = ColumnSchema(col_name, DataType(col_type, precision, scale))
            col_schemas.append(col_schema)
        # for col in self.open_table_for_read_result._metadata.columns:
        #     col_schema = ColumnSchema(col.name, col.type)
        #     col_schemas.append(col_schema)
        self.open_table_for_read_model = Model.from_columns(col_schemas)

    def next_row(self):
        row_source = next(self.open_table_for_read_result, None)
        if row_source is not None:
            row = Row()
            for i, col_schema in enumerate(self.open_table_for_read_model.columns):
                col_name = col_schema.name
                col_value = row_source[i]  # Access tuple by index instead of column name
                row.add_column(Column(col_name, col_value))
            return row
        else:
            return None
    
    def close_query(self):
        self.open_table_for_read_result.close()

    def close_table_for_read(self):
        self.open_table_for_read_result.close()

        

