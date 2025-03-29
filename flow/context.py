from datetime import datetime
import logging
from typing import Any, TYPE_CHECKING
from flow.variable_bindings import VariableBindings

if TYPE_CHECKING:
    from flow.job import Job
    from project.project import Project


class Context:
    def __init__(self, project: 'Project', job: 'Job'):
        self.variables = VariableBindings()
        self.project = project
        self.cur_execution_stack_entry = None
        self.job = job
        self.flow_name = None
        self.op_index_in_flow = None
        self.op_title = None

    # @classmethod
    # def from_project(cls, project):
    #     context = cls(project)
    #     context.variables = VariableBindings()
    #     context.project = project
    #     return context

    def clone(self):
        new_context = Context(self.project, self.job)
        new_context.variables = self.variables
        new_context.cur_execution_stack_entry = self.cur_execution_stack_entry
        new_context.flow_name = self.flow_name
        new_context.op_index_in_flow = self.op_index_in_flow
        new_context.op_title = self.op_title
        return new_context
    
    def set_variables(self, variables: VariableBindings):
        self.variables = variables

    def set_variable(self, name: str, value: Any):
        self.variables.set(name, value)
    
    def set_flow_name(self, flow_name: str):
        self.flow_name = flow_name
    
    def set_op_info(self, op_index_in_flow: int, op_title: str):
        self.op_index_in_flow = op_index_in_flow
        self.op_title = op_title
    
    
    def add_to_log_op_finished(self, logger: logging.Logger, message: str):
        start_time = self.cur_execution_stack_entry.start_time
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"{message} {duration}")
