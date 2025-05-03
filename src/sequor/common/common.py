import traceback
import sys

class Common:
    def __init__(self):
        pass

    @staticmethod
    def get_line_number(node, key_name):
        key_lc = node.lc.key(key_name) if key_name in node else None
        return key_lc[0] + 1 if key_lc else None
    
    @staticmethod
    def get_exception_traceback():
        """Get the exception's traceback as a string."""
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Format the traceback exactly like Python's default exception handler
        trace_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        return ''.join(trace_lines)
