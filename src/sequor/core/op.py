from typing import Any, Dict, List, Type, ClassVar, Union

from sequor.common.common import Common
from sequor.common.executor_utils import UserContext, load_user_function, render_jinja
from sequor.core.context import Context


class Op:
    """Base class for all operations"""
    # # Registry to store operation types and their corresponding classes
    # _registry: ClassVar[Dict[str, Type['Op']]] = {}
    
    # @classmethod
    # def register(cls, op_type: str):
    #     """Decorator to register operation classes"""
    #     def decorator(op_class: Type['Op']):
    #         # Register the operation class
    #         cls._registry[op_type] = op_class
    #         return op_class
    #     return decorator
    

    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        raise NotImplementedError("Subclasses must implement get_title")

    
    def run(self, context: Dict[str, Any], op_options: Dict[str, Any]) -> Dict[str, Any]:
        """Execute this operation with the given context"""
        raise NotImplementedError("Subclasses must implement run")

    # render: 0 - none, 1 - value only, 2 - expression only, 3 - both
    @staticmethod
    def get_parameter(context: Context, op_def: Dict[str, Any], name: str, is_required: bool = False, render: int = 0, location_desc: str = None) -> Any:
        param_value = op_def.get(name)
        param_expression = op_def.get(f"{name}_expression")
        param_expression_line = Common.get_line_number(op_def, f"{name}_expression")
        if render == 1:
            param_value = render_jinja(context, param_value)
        elif render == 2:
            param_expression = render_jinja(context, param_expression)
        elif render == 3:
            param_value = render_jinja(context, param_value)
            param_expression = render_jinja(context, param_expression)
        result_value = None
        if param_value and param_expression:
            raise ValueError(f"Both {name} and {name}_expression are specified in the definition. Only one of them can be specified.")
        elif param_expression:
            result_value = load_user_function(param_expression, f"{name}_expression", param_expression_line)
        elif param_value:
            result_value = param_value
        else:
            if is_required:
                err_msg = f"{name} or {name}_expression must be specified"
                if location_desc:
                    err_msg = err_msg + f" in {location_desc}"
                raise ValueError(err_msg)
        return result_value
    
    # render: 0 - none, 1 - value only; no need to render expression as it is already a compiled function
    @staticmethod
    def eval_parameter(context: Context, value: Any, render: int = 0, null_literal: bool = False) -> Any:
        user_context = UserContext(context)
        if value and callable(value):
            res = value(user_context)
        else:
            res = render_jinja(context, value, null_literal) if render == 1 else value
        return res

    def get_child_blocks(self) -> List[Dict[str, List['Op']]]:
        return []
    
    def get_id(self) -> Union[str, None]:
        id = self.op_def.get('id')
        return id