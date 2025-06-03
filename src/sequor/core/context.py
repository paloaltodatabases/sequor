from datetime import datetime
import logging
from typing import Any, TYPE_CHECKING
from sequor.core.flow_log_entry import FlowLogEntry
from sequor.core.variable_bindings import VariableBindings

# if TYPE_CHECKING:
#     from sequor.core.job import Job
#     from sequor.project.project import Project


class Context:
    def __init__(self, env: 'Environment', project: 'Project', job: 'Job'):
        self.env = env
        self.project = project
        self.cur_execution_stack_entry = None
        self.job = job
        self.variables = VariableBindings()
        self.flow_log = []


        # flow that is currently executing:
        self.flow_type_name = None # can be: flow, if (for IfOp), None (for ForEachOp, WhileOp)
        self.flow_name = None
        self.flow_step_index = None
        self.flow_step_index_name = None
    
    # Used only for accessing project and env variables - not for execution
    @classmethod
    def from_project(cls, project: 'Project'):
        return cls(project, None)

    def clone(self):
        new_context = Context(self.env, self.project, self.job)
        new_context.variables = self.variables
        new_context.cur_execution_stack_entry = self.cur_execution_stack_entry
        new_context.flow_type_name = self.flow_type_name
        new_context.flow_name = self.flow_name
        new_context.flow_step_index = self.flow_step_index
        new_context.flow_step_index_name = self.flow_step_index_name
        new_context.flow_log = self.flow_log
        return new_context
    
    def set_variables(self, variables: VariableBindings):
        self.variables = variables

    def set_variable(self, name: str, value: Any):
        self.variables.set(name, value)
    
    def get_variable_value(self, name: str):
        value = self.variables.get(name)
        if value is None:
            value = self.project.get_variable_value(name)
        if value is None:
            value = self.env.get_variable_value(name)
        return value
    
    def set_flow_info(self, flow_type_name: str, flow_name: str):
        self.flow_type_name = flow_type_name
        self.flow_name = flow_name
    
    def set_flow_step_info(self, index: int, index_name: str = None ):
        self.flow_step_index = index
        self.flow_step_index_name = index_name

    
    def add_to_log_op_finished(self, logger: logging.Logger, message: str):
        start_time = self.cur_execution_stack_entry.start_time
        end_time = datetime.now()
        duration = end_time - start_time
        self.flow_log.append(FlowLogEntry(message, start_time, end_time))
        logger.info(f"{message} {duration}")

    
