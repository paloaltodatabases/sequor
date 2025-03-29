import logging
from typing import Any, Dict

from common.executor_utils import render_jinja
from flow.op import Op


@Op.register('set_variable')
class SetVariableOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def run(self, context):
        logger = logging.getLogger("sequor.ops.set_variable")
        self.op_def = render_jinja(context, self.op_def)
        var_name = self.op_def.get('name')
        logger.info(f"Setting variable: {var_name}")
        var_value = self.op_def.get('value')
        context.set_variable(var_name, var_value)
        context.add_to_log_op_finished(logger, f"Finished. Variable {var_name} set to: {var_value}")