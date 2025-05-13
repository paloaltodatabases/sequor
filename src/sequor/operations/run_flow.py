import logging
from sequor.core.context import Context
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.core.variable_bindings import VariableBindings
from sequor.source.table_address import TableAddress


# @Op.register('run_flow')
class RunFlowOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        op_title = self.op_def.get('title')
        if (op_title is not None):
            title = self.name + ": " + op_title
        else:
            title = self.name + ": " + self.op_def.get('flow') if self.op_def.get('flow') else "unknown"
        return title

    def run(self, context: Context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.run_flow")
        self.op_def = render_jinja(context, self.op_def)
        flow_name = self.op_def.get('flow')
        logger.info(f"Starting flow: {flow_name}")

        start_step = self.op_def.get('start_step')
        # Safely cast start_step to int with error handling
        try:
            start_step_int = int(start_step) if start_step is not None else 0
            if start_step_int < 0:
                raise ValueError(f"start_step cannot be negative: {start_step}")
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid start_step value '{start_step}'. Must be a non-negative integer: {str(e)}")
        
        parameters_def = self.op_def.get('parameters', {})
        # Clone the context to avoid mutating the original context
        new_context = context.clone()
        # Load parameters into a new variable bindings
        flow_parameters_bindings = VariableBindings()
        for param_name, param_value in parameters_def.items():
            flow_parameters_bindings.set(param_name, param_value)
        # The new context will only have the flow parameters
        # i.e. We do not pass local variables from the current context
        new_context.set_variables(flow_parameters_bindings)
        
        flow = self.proj.get_flow(flow_name)
        flow.run(new_context, start_step_int)
        
        logger.info(f"Finished flow: {flow_name}")