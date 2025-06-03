from typing import Any, Dict, List
from sqlalchemy import Connection
from sequor.core.context import Context
from sequor.core.user_error import UserError
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
    def __init__(self, proj):
        self.proj = proj
        # self.source_name = source_name
        # self.table_addrs = table_addrs
        self._conn_pool: List[TableAddressToConnectionMap] = []

    # def get_model(self, model_name: str, model_def: Dict[str, Any], table_name: str) -> None:
    #     model = None
    #     if model_def is not None:
    #         model = Model.from_model_def(model_def)
    #     elif model_name is not None:
    #         model_spec = self.proj.get_specification("model", model_name)
    #         model = Model(model_spec.spec_def)
    #     else:
    #         raise Exception(f"Either model name or model specification must be provided for table: {table_name}")
    #     return model

    def get_connection(self, context: Context, table_addr: TableAddress, write_mode: str) -> Connection:
        conn = None
        for mapping in self._conn_pool:
            if (mapping.table_addr.source_name == table_addr.source_name and
                ((mapping.table_addr.database_name is None and table_addr.database_name is None) or
                 (mapping.table_addr.database_name is not None and table_addr.database_name is not None and
                  mapping.table_addr.database_name == table_addr.database_name)) and
                ((mapping.table_addr.namespace_name is None and table_addr.namespace_name is None) or
                 (mapping.table_addr.namespace_name is not None and table_addr.namespace_name is not None and
                  mapping.table_addr.namespace_name == table_addr.namespace_name)) and
                mapping.table_addr.table_name == table_addr.table_name):
                conn = mapping.conn
                break
        if conn is None:
            source = self.proj.get_source(context, table_addr.source_name)
            if source is None:
                raise Exception(f"Source not found: {table_addr.source_name}")
            new_conn = source.connect();
            table_addr_sub = table_addr.clone() # because we want original tableLoc to be added to the mapping (before spaceName enrichment)
            if table_addr_sub.namespace_name is None:
                table_addr_sub.namespace_name = source.get_default_namespace_name()
            # model = self.get_model(model_name, model_def, table_addr_sub.table_name)
            model = Model.from_model_def(table_addr.model_def)
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

    def run(self, context: Context, tables: List[TableAddress]) -> None:  # List[Dict[str, Any]]
        # if isinstance(tables_def, dict): # data for tables defined in response.tables section of http_request op
        # elif isinstance(tables_def, list): # not just data but full tables (definition + data)
        # else:
        #     raise UserError(f"Unknown type of tables data. Must be a dict or a list: {type(tables_def)}")

        for table_addr in tables:
            # data_def = tables_def.get(table_addr.table_name)
            # if data_def is None:
            #     raise UserError(f"Data for the target table {table_addr.table_name} not found in the result returned by the HTTP response parser.")
            # data_def = table_def.get('data')
            # model_def = table_def.get('model')
            # model_name = None
            # if isinstance(model_def, str):
            #     model_name = model_def
            # else:
            #     model_name = None
            # model_def = {"columns": table_addr.columns_def}
            write_mode = table_addr.write_mode
            if write_mode is None:
                write_mode = "create"
            # if data_def is not None: # skip quietly if no data, we used it in InfoLink for HTTPRequest op but why?
            conn = self.get_connection(context, table_addr, write_mode)
            # insert data
            for record_def in table_addr.data:
                record = Row()
                for column_schema in conn.model.columns:
                    column_name = column_schema.name
                    column_value = str(record_def.get(column_name)) # need to convert to string because it can be any type returned by the source
                    column = Column(column_name, column_value)
                    record.add_column(column)
                conn.insert_row(record)

