import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.op import Op


# @Op.register('print')
class PrintOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        super().__init__(proj, op_def)

    def get_title(self) -> str:
        op_title = self.op_def.get('title')
        if (op_title is not None):
            title = self.name + ": " + op_title
        else:
            message = self.op_def.get('message')
            cut_off = 50
            if message is not None and len(message) > cut_off:
                message = message[:cut_off] + "..."
            title = self.name + ": " + message if message else "unknown"
        return title

    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.print")
        self.op_def = render_jinja(context, self.op_def)
        message = self.op_def.get('message')
        logger.info(f"Message: {message}")
        context.add_to_log_op_finished(logger, f"Finished")