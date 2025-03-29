from flow.context import Context
from typing import Any, Dict, List, Optional, Union
from flow.op import Op


class Flow:
    """A flow containing a sequence of operations or nested flows"""
    def __init__(self, name: str, description: Optional[str] = None):
        self.name = name
        self.description = description
        self.steps: List[Op] = []
    
    def add_step(self, step: Op) -> None:
        """Add an operation or a nested flow to this flow"""
        self.steps.append(step)
    
    def run(self, context: Context, start_step: int = 0):
        context.set_flow_name(self.name)
        """Execute all steps in the flow sequentially"""
        for op_index, op in enumerate(self.steps[start_step:], start=start_step):
            # op.run(context)
            context.set_op_info(op_index, op.name)
            context.job.run(context, op)