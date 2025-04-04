import logging
from typing import List
from flow.context import Context
from flow.execution_stack_entry import ExecutionStackEntry
from flow.op import Op
from flow.user_error import UserError
from project.project import Project


class Job:
    def __init__(self, project: Project, op: Op ):
        self.project = project
        self.op = op
        self.execution_stack = []

    def get_cur_stack_entry(self) -> ExecutionStackEntry:
        return self.execution_stack[-1]

    def run(self, logger: logging.Logger, mode: str | None = None):
        context = Context(self.project, self)
        try:
            self.run_op(context, self.op, mode)
        except Exception as e:
            cur_stack_entry = self.get_cur_stack_entry()

            # Build job stacktrace lines
            job_stacktrace_lines = []
            for i, entry in enumerate(self.execution_stack):
                # Generate indentation based on stack depth
                indent = " " * (i * 2)
                location = None
                # flow_type_name is None in ops with single block such as ForEachOp
                # flow_name is None in the initial op of a job
                if entry.flow_type_name is None:
                    location = ""
                else:
                    if entry.flow_name is None:
                        flow_name_str = ""
                    else:
                        flow_name_str = f" \"{entry.flow_name}\""
                    if entry.flow_step_index is None:
                        index_str = ""
                    else:
                        index_name = "step" if entry.flow_step_index_name is None else entry.flow_step_index_name
                        index_str = f"{index_name} {entry.flow_step_index + 1} "
                    location = f" [{index_str}in {entry.flow_type_name}{flow_name_str}]"
                log_str = f"{indent}{"-> " if i > 0 else ""}\"{entry.op_title}\"{location}"
                job_stacktrace_lines.append(log_str)

            # --- Aggregated job stacktrace for screen ---
            
            screen_message = f"Error in \"{cur_stack_entry.op_title}\": {str(e)}\nStacktrace (most recent op last):\n" + "\n".join(job_stacktrace_lines)
            logger.error(screen_message)

            # Only re-raise if it's not a UserException
            if not isinstance(e, UserError):
                raise e


    def run_op(self, context: Context, op: Op, mode: str | None = None):
        prev_execution_stack_entry = context.cur_execution_stack_entry
        stack_entry = ExecutionStackEntry(op.get_title(), context.flow_type_name, context.flow_name, context.flow_step_index, context.flow_step_index_name, prev_execution_stack_entry)
        self.execution_stack.append(stack_entry)
        context.cur_execution_stack_entry = stack_entry
        op.run(context, mode)
        context.cur_execution_stack_entry = prev_execution_stack_entry
        self.execution_stack.pop()

