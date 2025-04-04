# run_flow.py
import logging
import os
from pathlib import Path
import sys
from flow.context import Context
from flow.execution_stack_entry import ExecutionStackEntry
from flow.job import Job
from flow.user_error import UserError
from operations.run_flow import RunFlowOp
from project.project import Project
from operations.registry import register_all_operations
import typer
# import typer.core
# typer.core.rich = None

# Disable rich traceback: rich_traceback=False
app = typer.Typer(
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
    rich_markup_mode=None)
# app = typer.Typer()

@app.command()
def init(
    project_name: str = typer.Argument(..., help="Project to create (e.g., 'salesforce_enrichment')")
):
    pass

@app.command()
def run(
    flow_name: str = typer.Argument(..., help="Flow to run (e.g. 'myflow' or 'salesforce/account_sync')"),
    op_mode: str = typer.Option(None, "--op-mode", help="Operation-specific mode for debugging or diagnostics (e.g. 'preview_response' for http_request op)"),
    op_id: str = typer.Option(None, "--op-id", help="ID of the operation to run"),
    project_dir_cli: str = typer.Option(None, "--project-dir", "-p", help="Path to Sequor project"),
    env_dir_cli: str = typer.Option(None, "--env-dir", help="Path to environment directory"),
    env_name_cli: str = typer.Option(None, "--environment", "-e", help="Environment to use (dev, prod, etc.)"),
):


    try:
        # Setting directories
        env_dir_default = Path.home() / ".sequor"
        env_dir = env_dir_default if env_dir_cli is None else Path(env_dir_cli)
        current_dir_str = os.getcwd()
        project_dir_str = project_dir_cli if project_dir_cli is not None else current_dir_str
        project_dir = Path(project_dir_str)

        # Init logging
        # Stick to "what" in INFO, and "how" in DEBUG 
        log_dir = env_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "sequor.log"
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

        # Register all operations at program startup
        register_all_operations()

        # Initialize a project
        project = Project(project_dir)

        if op_id is not None:
            # execute a single op in the flow
            flow = project.get_flow(flow_name)
            op = flow.get_op_by_id(op_id)
            logger.info(f"op to executed found: {op}")
            job = Job(project, op)
            op_mode_list = [op_mode] if op_mode is not None else []
            job.run(logger, op_mode_list)
        else:
            # execute the whole flow
            run_flow_op_def = {
                "op": "run_flow",
                "flow": flow_name,
                "start_step": 0,
                "parameters": {}
            }
            op = RunFlowOp(project, run_flow_op_def)
            job = Job(project, op)
            job.run(logger)
    except Exception as e:
        # Only re-raise if it's not a UserException
        if isinstance(e, UserError):
            logger.error(str(e))
        else:
            raise e

def entrypoint():
    app()

if __name__ == "__main__":
    import sys
    # sys.argv = ["cli.py", "--help"]
    # sys.argv = ["cli.py", "run","bigcommerce_download_customers", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc"]
    # sys.argv = ["cli.py", "run","bigcommerce_download_customers", "--op-id", "get_customers", "--op-mode", "preview_response", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc"]
    sys.argv = ["cli.py", "run","shopify_download_customers", "--op-id", "get_products", "--op-mode", "preview_response", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc"]

    app()