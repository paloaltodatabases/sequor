from typing import Any, Dict, List
from sqlalchemy import Connection
from sequor.source.model import Model
from sequor.source.row import Row
from sequor.source.table_address import TableAddress
from sequor.source.column import Column

class TableAddressToConnectionMap:
    def __init__(self, table_addr: TableAddress, conn: Connection):
        self.table_addr = table_addr
        self.conn = conn


class DataLoader:
    """Class for loading data from data definition"""
    _conn_pool: List[TableAddressToConnectionMap] = []
    
    def __init__(self, proj, source_name: str, table_addr: TableAddress):
        self.proj = proj
        self.source_name = source_name
        self.table_addr = table_addr

    # def __enter__(self):
    #     return self
    #
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.close()

    def get_model(self, model_name: str, model_def: Dict[str, Any], table_name: str) -> None:
        model = None
        if model_def is not None:
            model = Model.from_model_def(model_def)
        elif model_name is not None:
            model_spec = self.proj.get_specification("model", model_name)
            model = Model(model_spec.spec_def)
        else:
            raise Exception(f"Either model name or model specification must be provided for table: {table_name}")
        return model

    def get_connection(self, source_name: str, table_addr: TableAddress, model_name: str, model_def: Dict[str, Any], write_mode: str) -> Connection:
        conn = None
        for mapping in self._conn_pool:
            if (((mapping.table_addr.database_name is None and table_addr.database_name is None) or
                 (mapping.table_addr.database_name is not None and table_addr.database_name is not None and
                  mapping.table_addr.database_name == table_addr.database_name)) and
                ((mapping.table_addr.namespace_name is None and table_addr.namespace_name is None) or
                 (mapping.table_addr.namespace_name is not None and table_addr.namespace_name is not None and
                  mapping.table_addr.namespace_name == table_addr.namespace_name)) and
                mapping.table_addr.table_name == table_addr.table_name):
                conn = mapping.conn
                break
        if conn is None:
            source = self.proj.get_source(source_name)
            if source is None:
                raise Exception(f"Source not found: {source_name}")
            new_conn = source.connect();
            table_addr_sub = table_addr.clone() # because we want original tableLoc to be added to the mapping (before spaceName enrichment)
            if table_addr_sub.namespace_name is None:
                table_addr_sub.namespace_name = source.get_default_namespace_name()
            model = self.get_model(model_name, model_def, table_addr_sub.table_name)
            if write_mode == "create":
                new_conn.drop_table(table_addr_sub)
                new_conn.create_table(table_addr_sub, model)
            elif write_mode == "append":
                pass
            else:
                raise Exception(f"Unknown write mode: {write_mode}")

            self._conn_pool.append(TableAddressToConnectionMap(table_addr, new_conn)) # notice that we use "table_addr" not "table_addr_sub"
            new_conn.open_table_for_insert(table_addr_sub, model)
            new_conn.model = model # used in run() to create records to insert
            conn = new_conn # because we return "conn" to the caller and we want it to be the newly created connection
        return conn

    def close(self):
        for mapping in self._conn_pool:
            if mapping.conn is not None:
                mapping.conn.close_table_for_insert()
                mapping.conn.close()

    def run(self, tables_def: List[Dict[str, Any]]) -> None:
        for table_def in tables_def:
            source_name = table_def.get('source')
            if source_name is None:
                source_name = self.table_addr.source_name
            database_name = table_def.get('database')
            if database_name is None:
                database_name = self.table_addr.database_name
            namespace_name = table_def.get('namespace')
            if namespace_name is None:
                namespace_name = self.table_addr.namespace_name
            table_name = table_def.get('table')
            if table_name is None:
                table_name = self.table_addr.table_name
            table_addr = TableAddress(database_name, namespace_name, table_name)

            data_def = table_def.get('data')
            model_def = table_def.get('model')
            model_name = None
            if isinstance(model_def, str):
                model_name = model_def
            else:
                model_name = None
            write_mode = table_def.get('write_mode')
            if write_mode is None:
                write_mode = "create"
            if data_def is not None: # skip quitely if no data, we used it in InfoLink for HTTPRequest op but why?
                conn = self.get_connection(source_name, table_addr, model_name, model_def, write_mode)
                # insert data
                for record_def in data_def:
                    record = Row()
                    for column_schema in conn.model.columns:
                        column_name = column_schema.name
                        column_value = str(record_def.get(column_name)) # need to convet to string because it can be any type returned by the source
                        column = Column(column_name, column_value)
                        record.add_column(column)
                    conn.insert_row(record)

