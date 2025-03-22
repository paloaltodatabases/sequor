from typing import Any, Dict, List, Optional, Union

from flow.op import Op


class Flow:
    """A flow containing a sequence of operations or nested flows"""
    def __init__(self, id: str, description: Optional[str] = None):
        self.id = id
        self.description = description
        self.steps: List[Op] = []
    
    def add_step(self, step: Op) -> None:
        """Add an operation or a nested flow to this flow"""
        self.steps.append(step)
    
    def run(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute all steps in the flow sequentially"""
        if context is None:
            context = {}
        
        result = {}
        for step in self.steps:
            step_result = step.run(context)
            # Update context with step result for next steps
            context.update(step_result)
            result.update(step_result)
        
        return result