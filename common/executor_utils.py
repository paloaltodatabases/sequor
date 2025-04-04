import builtins
import logging
from typing import List
from flow.context import Context
from jinja2 import Template, StrictUndefined

from flow.execution_stack_entry import ExecutionStackEntry
from flow.user_error import UserError

def build_jinja_context(context: Context):
    def var(name):
        try:
            return context.variables.get(name)
        except KeyError:
            raise UserError(f"Variable '{name}' is not defined in the context")
    
    return {
        "var": var
    }

# Utility function to render a string with Jinja
def render_jinja_str(template_str, jinja_context):
    return Template(template_str, undefined=StrictUndefined).render(jinja_context)

# Recursively render all values in parsed YAML
def render_jinja(context, any_def):
    jinja_context = build_jinja_context(context)
    return _render_jinja_helper(any_def, jinja_context)

def _render_jinja_helper(any_def, jinja_context):
    if isinstance(any_def, str):
        return render_jinja_str(any_def, jinja_context)
    elif isinstance(any_def, dict):
        return {k: _render_jinja_helper(v, jinja_context) for k, v in any_def.items()}
    elif isinstance(any_def, list):
        return [_render_jinja_helper(v, jinja_context) for v in any_def]
    else:
        return any_def  # raw number, boolean, None, etc.


def get_restricted_builtins():
    # Whitelist only safe functions:
    safe_builtin_names = [
        'len', 'range', 'str', 'int', 'float', 'bool', 'dict', 'list', 'sum', 'min', 'max', 'abs', 'enumerate', 'zip', 'sorted', 'any', 'all'
    ]
    return {name: getattr(builtins, name) for name in safe_builtin_names}

def load_user_function(function_code: str, key_name: str, function_name: str = "evaluate", extra_globals=None):
    """
    Compile and safely execute user-defined Python function from string.
    
    :param function_code: Python function as string
    :param function_name: Name of the function to load from the executed code
    :param extra_globals: Optional dictionary of additional safe global objects (eg. json)
    :return: Callable function
    """

    # Compile the code first to catch syntax errors
    try:
        compiled_code = compile(function_code, filename=key_name, mode="exec")
    except SyntaxError as e:
        raise ValueError(f"Syntax error in the user function in {key_name}: {e}")

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
        raise RuntimeError(f"Error executing user code in {key_name}: {e}")

    # Retrieve the function object
    user_function = local_namespace.get(function_name)
    if not user_function:
        raise ValueError(f"Function {function_name} not found in user code")

    # Attach the source code and filename to the function for better error reporting
    user_function.__source_code__ = function_code
    user_function.__filename__ = key_name

    return user_function

