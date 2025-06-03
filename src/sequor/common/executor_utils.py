import builtins
import logging
from typing import Any, Callable, List, NamedTuple
from sequor.core.context import Context
from jinja2 import Template, StrictUndefined

from sequor.core.execution_stack_entry import ExecutionStackEntry
from sequor.core.user_error import UserError

def set_variable_from_def(context: Context, name: str, value_def: Any):
    if isinstance(value_def, dict):
        if not ("value" in value_def):
            raise UserError(f"Setting variable \"{name}\" with a dict that has no \"value\" key: {str(value_def)}")
        var_value = value_def["value"]
        if not ("scope" in value_def):
            raise UserError(f"Setting variable \"{name}\" with a dict that has no \"scope\" key: {str(value_def)}")
        var_scope = value_def["scope"]
        if var_scope not in ["local", "project"]:
            raise UserError(f"Setting variable \"{name}\" with invalid scope: {var_scope}")
    else:
        var_value = value_def
        var_scope = "project"
    set_variable(context, name, var_value, var_scope)
    return var_value, var_scope

def set_variable(context: Context, name: str, value: Any, scope: str):
    if scope == "local":
        context.set_variable(name, value)
    elif scope == "project":
        context.project.set_variable(name, value)
    else:
        raise UserError(f"Setting variable \"{name}\" in invalid scope: {scope}")

class UserSourcesAPI:
    def __init__(self, context, user_context):
        self.context = context
        self.user_context = user_context
    
    def query(self, source_name: str, query: str, database_name: str = None, namespace_name: str = None):
        source = self.context.project.get_source(self.context, source_name)
        result = []
        with source.connect() as conn:
            conn.open_query(query)
            while row := conn.next_row():
                result.append(row)
        return result
    
class UserContext:
    def __init__(self, context: Context):
        self.context = context
        self.sources = UserSourcesAPI(self.context, self)

    def is_var_defined(self, name):
        value = self.context.get_variable_value(name)
        if value is None:
            return False
        else:
            return True
    
    def var(self, name):
        value = self.context.get_variable_value(name)
        if value is None:
            raise UserError(f"Variable '{name}' is not defined")
        return value
    
    def query(self, source_name: str, query: str, database_name: str = None, spacename_name: str = None):
        source = self.proj.get_source(self.context, source_name)
        result = []
        with source.connect() as conn:
            conn.open_query(query)
            row_count = 0
            row = conn.next_row()
            while row is not None:
                row_count += 1
                row = conn.next_row()
                result.append(row)
        return result


def build_jinja_user_context(context: Context):
    def var(name):
        value = context.get_variable_value(name)
        if value is None:
            raise UserError(f"Variable '{name}' is not defined")
        return value
    return {
        "var": var
    }

# Recursively render all values in parsed YAML
def render_jinja(context, any_def, null_literal: bool = False):
    jinja_context = build_jinja_user_context(context)
    try:
        any_def_rendered = _render_jinja_helper(any_def, jinja_context, null_literal)
    except Exception as e:
        raise UserError(f"Error rendering Jinja template \"{str(e)}\" in definition: {str(any_def)}")
    return any_def_rendered

# Utility function to render a string with Jinja
def _render_jinja_str(template_str, jinja_context, null_literal: bool):
    str_rendered = Template(template_str, undefined=StrictUndefined).render(jinja_context)
    if null_literal and str_rendered == "__NULL__": # compare case sensitive to align with YAML which is case sensitive
        str_rendered = None
    return str_rendered



# def _render_jinja_helper(any_def, jinja_context):
#     if isinstance(any_def, str):
#         return render_jinja_str(any_def, jinja_context)
#     elif isinstance(any_def, dict):
#         return {k: _render_jinja_helper(v, jinja_context) for k, v in any_def.items()}
#     elif isinstance(any_def, list):
#         return [_render_jinja_helper(v, jinja_context) for v in any_def]
#     else:
#         return any_def  # raw number, boolean, None, etc.
def _render_jinja_helper(any_def, jinja_context, null_literal: bool):
    from ruamel.yaml.comments import CommentedMap, CommentedSeq
    import copy
    
    if isinstance(any_def, str):
        return _render_jinja_str(any_def, jinja_context, null_literal)
    elif isinstance(any_def, (CommentedMap, dict)):
        # For CommentedMap, create a shallow copy to preserve metadata
        if isinstance(any_def, CommentedMap):
            result = copy.copy(any_def)
            # Clear contents but keep metadata
            result.clear()
            # Fill with rendered values
            for k, v in any_def.items():
                result[k] = _render_jinja_helper(v, jinja_context, null_literal)
            return result
        else:
            return {k: _render_jinja_helper(v, jinja_context, null_literal) for k, v in any_def.items()}
    elif isinstance(any_def, (CommentedSeq, list)):
        # For CommentedSeq, create a shallow copy to preserve metadata
        if isinstance(any_def, CommentedSeq):
            result = copy.copy(any_def)
            # Clear contents but keep metadata
            result.clear()
            # Fill with rendered values
            for v in any_def:
                result.append(_render_jinja_helper(v, jinja_context, null_literal))
            return result
        else:
            return [_render_jinja_helper(v, jinja_context, null_literal) for v in any_def]
    else:
        return any_def  # raw number, boolean, None, etc.

def get_restricted_builtins():
    # Whitelist only safe functions:
    safe_builtin_names = [
        'len', 'range', 'str', 'int', 'float', 'bool', 'dict', 'list', 'sum', 'min', 'max', 'abs', 'enumerate', 'zip', 'sorted', 'any', 'all'
    ]
    return {name: getattr(builtins, name) for name in safe_builtin_names}

def user_function_error_message(e: Exception, key_name: str, line_in_code: int, line_in_yaml: int, auto_wrapped: bool, prefix: str = "Error"):
    if auto_wrapped:
        line_in_code = line_in_code - 1
    position_in_code = f"line {line_in_code} of code, " if line_in_code else ""
    absolute_line_in_yaml = line_in_yaml + line_in_code if line_in_code else line_in_yaml
    error_msg = f"{prefix} in {key_name} ({position_in_code}line {absolute_line_in_yaml} in YAML): {type(e).__name__}: {str(e)}"
    return error_msg

def load_user_function(function_code: str, key_name: str, line_in_yaml: int, function_params_def: str = "context", function_name: str = "evaluate", extra_globals=None):
    """
    Compile and safely execute user-defined Python function from string.
    
    :param function_code: Python function as string
    :param function_name: Name of the function to load from the executed code
    :param extra_globals: Optional dictionary of additional safe global objects (eg. json)
    :return: Callable function
    """
    auto_wrapped = False
    if not function_code.strip().startswith("def evaluate"):
        auto_wrapped = True
        # Properly indent the function body with 4 spaces
        indented_body = '\n'.join('    ' + line for line in function_code.splitlines())
        function_code = f"def evaluate({function_params_def}):\n{indented_body}"

    # Compile the code first to catch syntax errors
    try:
        compiled_code = compile(function_code, filename=key_name, mode="exec")
    except SyntaxError as e:
        line_in_code = e.lineno
        pure_msg = e.msg
        raise UserError(user_function_error_message(SyntaxError(pure_msg), key_name, line_in_code, line_in_yaml, auto_wrapped, "Syntax error"))
       

    # Setup sandboxed globals
    safe_globals = {
        "__builtins__": get_restricted_builtins()
    }

    # Optionally add safe standard libraries
    if extra_globals:
        safe_globals.update(extra_globals)

    local_namespace = {}

    # Execute in restricted environment
    try:
        # exec(compiled_code, safe_globals, local_namespace)
        exec(compiled_code, None, local_namespace)
    except Exception as e:
        # Get the traceback information
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        if tb:
            # Get the last frame from the traceback
            last_frame = tb[-1]
            # Format the error with file and line information
            error_msg = f"Error in {key_name} at line {last_frame.lineno}: {str(e)}"
            # Create a new exception with the same type but our custom message
            new_exc = type(e)(error_msg)
            # Preserve the original traceback
            new_exc.__traceback__ = e.__traceback__
            raise new_exc
        # raise RuntimeError(f"Error executing user code in {key_name}: {e}")
        raise UserError(user_function_error_message(e, key_name, None, line_in_yaml, auto_wrapped, "Error"))

    # Retrieve the function object
    user_function = local_namespace.get(function_name)
    if not user_function:
        fun_not_defined_error = UserError(f"Function {function_name} not found in user code")
        raise UserError(user_function_error_message(fun_not_defined_error, key_name, None, line_in_yaml, auto_wrapped, "Error"))

    # Attach the source code and filename to the function for better error reporting
    user_function.__source_code__ = function_code
    user_function.__filename__ = key_name
    user_function.__auto_wrapped__ = auto_wrapped

    return user_function


class UserFunction:
    def __init__(self, fun: Callable, line_in_yaml: int):
        self.fun: Callable = fun
        self.line_in_yaml: int = line_in_yaml # line number of the function definition in the yaml file

    def apply(self, *args, **kwargs):
        try:
            result = self.fun(*args, **kwargs)
        except Exception as e:
            # Get the traceback information
            import traceback
            tb_info = traceback.extract_tb(e.__traceback__)
            
            # Find the frame in the user's code (it will have the filename we attached)
            user_frame = None
            for frame in tb_info:
                if hasattr(self.fun, '__filename__') and frame.filename == self.fun.__filename__:
                    user_frame = frame
                    break
            
            if user_frame:
                # Format error with line information from the user's code
                # error_msg = f"Error in {self.fun.__filename__} (line {user_frame.lineno} of code, line {self.line_in_yaml} in YAML): {type(e).__name__}: {str(e)}"
                error_msg = user_function_error_message(e, self.fun.__filename__, user_frame.lineno, self.line_in_yaml, self.fun.__auto_wrapped__, "Error")
                new_exc = UserError(error_msg)
                # Preserve the original traceback
                new_exc.__traceback__ = e.__traceback__
                raise new_exc
            else:
                # If we couldn't find the frame, just pass through the original error
                raise
        return result

