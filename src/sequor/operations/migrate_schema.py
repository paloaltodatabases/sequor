import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.op import Op
from sequor.source.column_schema import ColumnSchema
from sequor.source.data_type import DataType
from sequor.source.model import Model
from sequor.source.table_address import TableAddress


class MigrateSchemaOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        title = self.name
        op_id = self.op_def.get('id')
        if (op_id is not None):
            title = self.name + ": " + op_id
        elif self.op_def.get('target_table') is not None:
            title = self.name + ": " + self.op_def.get('target_table')
        return title

    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.migrate_schema")
        self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting \"{self.get_title()}\"")
        target_source_name = self.op_def.get('target_source')
        target_database_name = self.op_def.get('target_database')
        target_namespace_name = self.op_def.get('target_namespace')
        target_table_name = self.op_def.get('target_table')
        columns_source_name = self.op_def.get('columns_source')
        columns_database_name = self.op_def.get('columns_database')
        columns_namespace_name = self.op_def.get('columns_namespace')
        columns_table_name = self.op_def.get('columns_table')

        target_table_addr = TableAddress(target_source_name, target_database_name, target_namespace_name, target_table_name)
        columns_table_addr = TableAddress(columns_source_name, columns_database_name, columns_namespace_name, columns_table_name)

        columns_model = None
        columns_source = self.proj.get_source(context, columns_source_name)
        with columns_source.connect() as columns_conn:
            columns_conn.open_table_for_read(columns_table_addr)
            columns_table_count = 0
            column_row = columns_conn.next_row()
            column_schemas = []
            while column_row is not None:
                columns_table_count += 1
                column_schema = ColumnSchema(column_row.get('name'), DataType(column_row.get('type')))
                column_schemas.append(column_schema)
                column_row=columns_conn.next_row()
            columns_model = Model.from_columns(column_schemas)

        target_source = self.proj.get_source(context, target_source_name)
        with target_source.connect() as target_conn:
            target_model = target_conn.get_model(target_table_addr)
            # add new columns
            for columns_column in columns_model.columns:
                target_column = target_model.get_column(columns_column.name)
                if target_column is None:
                    target_conn.add_column(target_table_addr, columns_column.name, columns_column.type)
            # drop removed columns
            for target_column in target_model.columns:
                columns_column = columns_model.get_column(target_column.name)
                if columns_column is None:
                    target_conn.drop_column(target_table_addr, target_column.name)

        context.add_to_log_op_finished(
            logger, f"Finished \"" + self.get_title() + "\"")
