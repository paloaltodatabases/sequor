# run_flow.py
import logging
from pathlib import Path
import sys
from common.executor_utils import print_stack_trace_to_logger
from flow.context import Context
from flow.execution_stack_entry import ExecutionStackEntry
from flow.job import Job
from operations.run_flow import RunFlowOp
from project.project import Project

log_dir = Path.home() / ".mytool" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_path = log_dir / "sequor.log"

# Stick to “what” in INFO, and “how” in DEBUG 
logging.basicConfig(
    level=logging.INFO,                         # default level
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",         # format for stdout
    handlers=[
        logging.StreamHandler(),                # prints to console
        logging.FileHandler(log_path)         # writes to log file
    ]
)

logger = logging.getLogger("sequor.cli")
logger.info("Starting CLI tool")

# CLI parameters
flow_name = "flow1"

# Initialize a project
project = Project("/Users/maximgrinev/myprogs/sequor_projects/misc")

# Construct an op to execute the flow
run_flow_op_def = {
    "op": "run_flow",
    "flow": flow_name,
    "start_step": 0,
    "parameters": {}
}
op = RunFlowOp(project, run_flow_op_def)

# Call a job that will execute the op
job = Job(project)
context = Context(project, job)
try:
    job.run(context, op)
except Exception as e:
    logger.error(f"Error executing flow: {str(e)}")
    logger.error("Stack trace:")
    import traceback
    error_traceback = traceback.format_exc()
    logger.error(error_traceback)
    print_stack_trace_to_logger(logger, job.execution_stack)
    sys.exit(1)
    # raise e

logger.info("Finished CLI tool")