from sequor.core.context import Context
from typing import Any, Dict, List, Optional, Union
from sequor.core.op import Op
from sequor.core.user_error import UserError


class Flow:
    """A flow containing a sequence of operations or nested flows"""
    def __init__(self, type_name: str, name: str = None, description: Optional[str] = None):
        self.type_name = type_name
        self.name = name
        self.description = description
        self.steps: List[Op] = []

    def add_step(self, step: Op) -> None:
        """Add an operation or a nested flow to this flow"""
        self.steps.append(step)
    
    def run(self, context: Context, start_step: int = 0, op_options: Dict[str, Any] = {}):
        """Execute all steps in the flow sequentially"""
        context.set_flow_info(self.type_name, self.name)
        for op_index, op in enumerate(self.steps[start_step:], start=start_step):
            # op.run(context)
            context.set_flow_step_info(op_index)
            context.job.run_op(context, op, op_options)
    
    # def get_child_blocks(self) -> List[Dict[str, List['Op']]]:
    #     for step in self.steps:
    #         op = Op.create(self.proj, step)

    def get_op_by_id(self, op_id: str) -> List[tuple['Op', List[tuple[str, int]]]]:
        start = [{'root': self.steps}]
        ops_found = self.find_op_by_id_helper(start, op_id)
        if len(ops_found) == 0:
            raise UserError(f"Operation with ID {op_id} not found in flow {self.name}")
        elif len(ops_found) > 1:
            # Build a string of all paths where the operation was found
            paths = []
            for op, path in ops_found:
                path_str = " -> ".join([f"{block_name}[{index}]" for block_name, index in path])
                paths.append(path_str)
            
            # Join all paths with commas for the error message
            paths_str = ", ".join(paths)
            raise UserError(f"Multiple operations with ID {op_id} found in flow {self.name}: {paths_str}")
        return ops_found[0][0]

    def find_op_by_id_helper(self,blocks: List[Dict[str, List['Op']]], op_id: str) -> List[tuple['Op', List[tuple[str, int]]]]:
        """Recursively find all operations with the given ID and their paths.
        
        Args:
            blocks: List of dictionaries where each dictionary contains a block name and list of operations
            op_id: The ID to search for
            
        Returns:
            List of tuples containing (operation, path) where path is a list of (block_name, index) tuples
        """
        results = []
        
        for block in blocks:
            for block_name, ops in block.items():
                for index, op in enumerate(ops):
                    # Check if current op matches the ID
                    if op.get_id() == op_id:
                        results.append((op, [(block_name, index)]))
                    
                    # Recursively search in child blocks
                    child_blocks = op.get_child_blocks()
                    if child_blocks:
                        child_results = self.find_op_by_id_helper(child_blocks, op_id)
                        for child_op, child_path in child_results:
                            # Prepend current block and index to the path
                            full_path = [(block_name, index)] + child_path
                            results.append((child_op, full_path))
        
        return results
        
                