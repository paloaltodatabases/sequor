import logging
from typing import Any, Dict

from common.executor_utils import render_jinja
from flow.op import Op


@Op.register('print')
class PrintOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def run(self, context):
        logger = logging.getLogger("sequor.ops.print")
        self.op_def = render_jinja(context, self.op_def)
        message = self.op_def.get('message')
        logger.info(f"Message: {message}")
        context.add_to_log_op_finished(logger, f"Finished")