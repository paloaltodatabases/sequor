import logging
from typing import Any, Dict, List
from sequor.common.common import Common
from sequor.core.context import Context
from sequor.core.environment import Environment
from sequor.core.execution_stack_entry import ExecutionStackEntry
from sequor.core.op import Op
from sequor.core.user_error import UserError
from sequor.project.project import Project
import uuid

logger = logging.getLogger("sequor.job")


class Job:
    def __init__(self, env: Environment, project: Project, op: Op, options: dict):
        self.env = env
        self.project = project
        self.op = op
        self.execution_stack = []
        self.options = options


    def get_cur_stack_entry(self) -> ExecutionStackEntry:
        if len(self.execution_stack) == 0:
            return None
        return self.execution_stack[-1]

    # logger: logging.Logger,
    def run(self, op_options: Dict[str, Any]):
        context = Context(self.env, self.project, self)
        try:
            self.run_op(context, self.op, op_options)
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
                log_str = f"{indent}{'-> ' if i > 0 else ''}\"{entry.op_title}\"{location}"
                job_stacktrace_lines.append(log_str)

            job_stacktrace = Common.get_exception_traceback()
            if self.options.get("show_stacktrace"):
                logger.error("Python stacktrace:\n" + job_stacktrace)
            # cur_stack_entry can be None if the error happens in the initial op of a job: e.g. in get_title() of an op during stack_entry creation
            if cur_stack_entry is not None:
                error_msg = f"Error in \"{cur_stack_entry.op_title}\": {str(e)}"
            else:
                error_msg = f"Error: {str(e)}"
            if self.options.get("disable_flow_stacktrace") is not None and not self.options["disable_flow_stacktrace"]:
                error_msg = error_msg + "\nStacktrace (most recent op last):\n" + "\n".join(job_stacktrace_lines)
            logger.error(error_msg)
        flow_log_dict = [entry.to_dict() for entry in context.flow_log]
        return {"flow_log": flow_log_dict}




    def run_op(self, context: Context, op: Op, op_options: Dict[str, Any]):
        prev_execution_stack_entry = context.cur_execution_stack_entry
        stack_entry = ExecutionStackEntry(op.get_title(), context.flow_type_name, context.flow_name, context.flow_step_index, context.flow_step_index_name, prev_execution_stack_entry)
        self.execution_stack.append(stack_entry)
        context.cur_execution_stack_entry = stack_entry
        op.run(context, op_options)
        context.cur_execution_stack_entry = prev_execution_stack_entry
        self.execution_stack.pop()

