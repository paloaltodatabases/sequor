import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.context import Context
from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.core.registry import create_op



# @Op.register('if')
class IfOp(Op):
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
        logger = logging.getLogger("sequor.ops.if")
        # in "control statement" type of op we cannot render the whole op_def as it contains other ops
        # for which context is not available yet -> we will render each parameter individually
        # self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting")
        conditions_def = self.op_def.get('conditions')
        block_op = None
        is_condition_met = False
        condition_met_index = None
        for index, conditions_def in enumerate(conditions_def):
            condition_value_def = conditions_def.get("condition")
            condition = Op.get_parameter(context, conditions_def, 'condition', is_required=True, render=3)
            then_steps_def = conditions_def.get('then')
            if str(condition).strip().lower() == "true":
                block_op_def = {
                    "op": "block",
                    "op_name_alias": f"condition_block",
                    "title": f"{condition_value_def}",
                    "steps": then_steps_def
                }
                block_op = create_op(context.project, block_op_def)
                # flow = context.project.build_flow_from_block_def("then", None, then_block)
                is_condition_met = True
                condition_met_index = index
                break
        if not is_condition_met:
            else_steps_def = self.op_def.get('else')
            # flow = context.project.build_flow_from_block_def("else", None, else_steps_def)
            block_op_def = {
                "op": "block",
                "op_name_alias": f"else_block",
                "steps": else_steps_def
            }
            block_op = Op.op_from_def(context.project, block_op_def)
        # flow.run(context)
        new_context = context.clone()
        new_context.set_flow_info("if", None)
        if is_condition_met:
            new_context.set_flow_step_info(condition_met_index, "condition")
        else:
            new_context.set_flow_step_info(None)
        context.job.run_op(new_context, block_op, None)
        logger.info(f"Finished")