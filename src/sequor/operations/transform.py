import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.op import Op
from sequor.source.table_address import TableAddress


# @Op.register('transform')
class TransformOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        op_id = self.op_def.get('id')
        if (op_id is not None):
            title = self.name + ": " + op_id
        else:
            title = self.name + ": " + self.op_def.get('target_table') if self.op_def.get('target_table') else "unknown"
        return title

    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.transform")
        self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting \"{self.get_title()}\"")
        source_name = self.op_def.get('source')
        query = self.op_def.get('query')
        target_database = self.op_def.get('target_database')
        target_namespace = self.op_def.get('target_namespace')
        target_table = self.op_def.get('target_table')
        
        # Create TableAddress object from target_table string
        target_table_addr = TableAddress(source_name, target_database, target_namespace, target_table)

        source = self.proj.get_source(context,source_name)
        with source.connect() as conn:
            conn.drop_table(target_table_addr)
            createTableSql = source.get_create_table_sql(query, target_table_addr)
            print(f"Executing: {createTableSql}")
            conn.execute_update(createTableSql)
    
        # logger.info(f"Finished \"{self.get_title()}\"")
        context.add_to_log_op_finished(
            logger, f"Finished \"" + self.get_title() + "\"")
