from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.context import Context
from sequor.source.table_address import TableAddress

class Source:
    def __init__(self, context: 'Context', name: str, source_def: Dict[str, Any]):
        self.context = context
        self.name = name 
        self.source_def = source_def
        self.source_rendered_def = None

    def get_rendered_def(self):
        if self.source_rendered_def is None:
            # if context is None:
            #     context = Context.from_project(self.project)
            self.source_rendered_def = render_jinja(self.context, self.source_def)
        return self.source_rendered_def

    def connect(self):
        raise NotImplementedError("Subclasses must implement connect()")

    def get_qualified_name(self, table_addr: TableAddress):
        raise NotImplementedError("Subclasses must implement get_qualified_name()")

    def get_default_namespace_name(self):
        raise NotImplementedError("Subclasses must implement get_default_namespace_name()")

    @staticmethod
    def get_parameter(context, source_def: Dict[str, Any], name: str, is_required: bool = False, render: bool = False) -> Any:
        param_value = source_def.get(name)
        if render:
            param_value = render_jinja(context, param_value)
        result_value = None
        if param_value:
            result_value = param_value
        else:
            if is_required:
                raise Exception(f"{name} must be specified in source definition.")
        return result_value