import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.context import Context
from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.source.table_address import TableAddress


# @Op.register('block')
class BlockOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        super().__init__(proj, op_def)

    def get_title(self) -> str:
        op_title = self.op_def.get('title')
        op_name_alias = self.op_def.get('op_name_alias')
        op_name = self.name if op_name_alias is None else op_name_alias
        if (op_title is not None):
            title = op_name + ": " + op_title
        else:
            title = op_name
        return title

    def run(self, context: Context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.block")
        # self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting")
        steps_def = self.op_def.get('steps')
        flow = context.project.build_flow_from_block_def(steps_def)
        op_name_alias = self.op_def.get('op_name_alias')
        if op_name_alias is not None:
            flow.type_name = op_name_alias
        # new_context = context.clone()
        # new_context.set_flow_info("block", None)
        flow.run(context)
        logger.info(f"Finished")