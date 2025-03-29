from contextvars import Context
from flow.execution_stack_entry import ExecutionStackEntry
from flow.op import Op
from project.project import Project


class Job:
    def __init__(self, project: Project ):
        self.project = project
        self.execution_stack = []

    def run(self, context: Context, op: Op):
        prev_execution_stack_entry = context.cur_execution_stack_entry
        stack_entry = ExecutionStackEntry(op.name, context.flow_name, context.op_index_in_flow, prev_execution_stack_entry)
        self.execution_stack.append(stack_entry)
        context.cur_execution_stack_entry = stack_entry
        op.run(context)
        context.cur_execution_stack_entry = prev_execution_stack_entry
        self.execution_stack.pop()

