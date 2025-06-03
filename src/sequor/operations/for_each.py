import logging
from typing import Any, Dict

from sequor.core.context import Context
from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.core.registry import create_op
from sequor.source.table_address import TableAddress


# @Op.register('for_each')
class ForEachOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        super().__init__(proj, op_def)

    def get_title(self) -> str:
        op_title = self.op_def.get('title')
        if (op_title is not None):
            title = self.name + ": " + op_title
        else:
            title = self.name
        return title

    def run(self, context: Context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.for_each")
        # in "control statement" type of op we cannot render the whole op_def as it contains other ops
        # for which context is not available yet -> we will render each parameter individually
        # self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting")
        source_name = Op.get_parameter(context, self.op_def, 'source', is_required=True, render=3)
        database_name = Op.get_parameter(context, self.op_def, 'database', is_required=False, render=3)
        namespace_name = Op.get_parameter(context, self.op_def, 'namespace', is_required=False, render=3)
        table_name= Op.get_parameter(context, self.op_def, 'table', is_required=True, render=3)
        table_address = TableAddress(source_name, database_name, namespace_name, table_name)
        var_name= Op.get_parameter(context, self.op_def, 'as', is_required=True, render=3)

        steps_def = self.op_def.get('steps')
        block_op_def = {
            "op": "block",
            "op_name_alias": f"for_each_block",
            "steps": steps_def
        }
        block_op = create_op(context.project, block_op_def)
        new_context = context.clone()
        new_context.set_flow_info("for_each", None)
        new_context.set_flow_step_info(None)

        row_count = 0
        self.source = self.proj.get_source(context,source_name)
        with self.source.connect() as conn:
            conn.open_table_for_read(table_address)
            row = conn.next_row()
            while row is not None:
                row_count += 1
                new_context.set_variable(var_name, row)
                context.job.run_op(new_context, block_op, None)
                row = conn.next_row()


        logger.info(f"Finished. Processed {row_count} rows")