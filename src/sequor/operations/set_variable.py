import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja, set_variable, set_variable_from_def
from sequor.core.op import Op


# @Op.register('set_variable')
class SetVariableOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        op_id = self.op_def.get('id')
        if (op_id is not None):
            title = self.name + ": " + op_id
        else:
            title = self.name
        return title

    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.set_variable")
        self.op_def = render_jinja(context, self.op_def)
        # var_name = Op.get_parameter(context, self.op_def, 'name', is_required=True)
        # logger.info(f"Setting variable: {var_name}")
        # var_value = Op.get_parameter(context, self.op_def, 'value', is_required=True)
        # var_scope = Op.get_parameter(context, self.op_def, 'scope', is_required=False)
        # if var_scope is None:
        #     var_scope = "project"
        # set_variable(context, var_name, var_value, var_scope)
        # msg = f"Finished. Variable \"{var_name}\" set in scope \"{var_scope}\" to: {var_value}"

        set_def = Op.get_parameter(context, self.op_def, 'set', is_required=True, render=0, location_desc="set_variable")
        vars_set = []
        for var_name, var_value in set_def.items():
            var_value_set, var_scope_set = set_variable_from_def(context, var_name, var_value)
            var_set_str = f"\"{var_name}\" to \"{var_value_set}\" in {var_scope_set} scope"
            logger.info(f"Setting variable: " + var_set_str)
            vars_set.append(var_set_str)
        # msg = f"Finished. Variables set: " + ", ".join(vars_set)
        # context.add_to_log_op_finished(logger, msg)
        context.add_to_log_op_finished(
            logger, f"Finished \"" + self.get_title() + "\": variables set: " + ", ".join(vars_set))
