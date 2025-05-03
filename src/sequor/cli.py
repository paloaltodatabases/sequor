# run_flow.py
import logging
import os
from pathlib import Path
import sys
import yaml
from sequor.common.common import Common
from sequor.core.context import Context
from sequor.core.environment import Environment
from sequor.core.execution_stack_entry import ExecutionStackEntry
from sequor.core.job import Job
from sequor.core.user_error import UserError
from sequor.operations.run_flow import RunFlowOp
from sequor.project.project import Project
from sequor.operations.registry import register_all_operations
from sequor.common import telemetry
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
def version():
    from sequor import __version__
    typer.echo(f"Sequor version: {__version__}")

@app.command()
def env_init(
    env_dir: str = typer.Argument(None, help="Path to create environment directory (default: ~/sequor-env)"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing environment directory", is_flag=True)
):
    """Initialize a Sequor environment directory for storing credentials, logs, and state."""
    # If no env_dir provided, use default
    if env_dir is None:
        env_dir = os.path.expanduser("~/sequor-env")
    
    # Create directory structure and initial files
    pass

@app.command()
def init(
    project_name: str = typer.Argument(..., help="Project to create (e.g., 'salesforce_enrichment')")
):
    pass

@app.command()
def run(
    flow_name: str = typer.Argument(..., help="Flow to run (e.g. 'myflow' or 'salesforce/account_sync')"),
    # op_mode: str = typer.Option(None, "--op-mode", help="Operation-specific mode for debugging or diagnostics (e.g. 'preview_response' for http_request op)"),
    project_dir_cli: str = typer.Option(None, "--project-dir", "-p", help="Path to Sequor project"),
    env_dir_cli: str = typer.Option(None, "--env-dir", help="Path to environment directory"),

    # Job-level options
    disable_flow_stacktrace: bool = typer.Option(False, "--disable-flow-stacktrace", help="Show the execution path through the flow operations", is_flag=True),
    show_stacktrace: bool = typer.Option(False, "--stacktrace", help="Show the Python exception stack trace", is_flag=True),

    op_id: str = typer.Option(None, "--op-id", help="ID of the operation to run"),

    # http_request op specific options
    debug_foreach_record: str = typer.Option(None, "--debug-foreach-record", help="Run with a test for_each record specified as JSON object (or records if batching is enabled as JSON array). The record(s) should match the structure expected by the operation. Example: --debug-test-record='{\"email\":\"test@example.com\"}'"),
    debug_request_preview_trace: bool = typer.Option(False, "--debug-request-preview-trace", help="Run only HTTP request part and show HTTP request trace", is_flag=True),
    debug_request_preview_pretty: bool = typer.Option(False, "--debug-request-preview-pretty", help="Run only HTTP request part and show pretty trace", is_flag=True),
    debug_response_parser_preview: bool = typer.Option(False, "--debug-response-parser-preview", help="Show parser result without applying it", is_flag=True),
):
    logger = logging.getLogger("sequor.cli")
    logger.info("Starting CLI tool")

    try:
        # Setting project dir
        if project_dir_cli:
            project_dir = Path(project_dir_cli)
            if not project_dir.exists():
                raise UserError(f"Project directory passed as CLI --project-dir argument does not exist: {project_dir_cli}")            
        else:
            current_dir = os.getcwd()
            project_dir = Path(current_dir)

        # Setting env dir
        env_os_var = os.environ.get("SEQUOR_ENV")
        project_env_file = project_dir / "sequor_env.yaml"
        default_env_dir = Path.home() / "sequor_env"
        if env_dir_cli:
            env_dir = Path(env_dir_cli)
            if not env_dir.exists():
                raise UserError(f"Environment directory passed as CLI --env-dir argument does not exist: {env_dir_cli}")
        elif project_env_file.exists():
            with project_env_file.open("r") as f:
                project_env_data = yaml.safe_load(f)
                if "env_dir" not in project_env_data:
                    raise UserError(f"'env_dir' key not found in project environment file: {project_env_file}")
                env_dir = Path(project_env_data["env_dir"])
                if not env_dir.exists():
                    raise UserError(f"Environment directory referenced in project environment file sequor_env.yaml does not exist: {env_dir}")
        elif env_os_var:
            env_dir = Path(env_os_var)
            if not env_dir.exists():
                raise UserError(f"Environment directory passed as SEQUOR_ENV environment variable does not exist: {env_os_var}")
        elif default_env_dir.exists():
            env_dir = Path.home() / "sequor_env"
        else:
            raise UserError(f"Environment directory not found. Please specify it using --env-dir argument, SEQUOR_ENV environment variable, or in project sequor_env.ymal. To create it, run 'sequor env_init [PATH]'")




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

        # Set up telemetry
        telemetry.basicConfig(
            api_key = "phc_XBYG9x8aUaBlQGhNhRwEwJbQ9xCzWs05Cy671pzjxvs", 
            host = "https://us.i.posthog.com", 
            user_id_file = Path.home() / ".sequor_user_id")
        telemetry_logger = telemetry.getLogger("sequor.cli")
        telemetry_logger.event("cli_start", prop1="value1")


        # Register all operations at program startup
        register_all_operations()

        # Initialize an environment
        env = Environment(env_dir)

        # Initialize a project
        project = Project(env, project_dir)

        op_options = {
            "debug_foreach_record": debug_foreach_record,
            "debug_request_preview_trace": debug_request_preview_trace,
            "debug_request_preview_pretty": debug_request_preview_pretty,
            "debug_response_parser_preview": debug_response_parser_preview
        }

        if op_id is not None:
            # execute a single op in the flow
            flow = project.get_flow(flow_name)
            op = flow.get_op_by_id(op_id)
        else:
            # execute the whole flow
            run_flow_op_def = {
                "op": "run_flow",
                "flow": flow_name,
                "start_step": 0,
                "parameters": {}
            }
            op = RunFlowOp(project, run_flow_op_def)
        job = Job(env, project, op, {"disable_flow_stacktrace": disable_flow_stacktrace, "show_stacktrace": show_stacktrace})
        job.run(logger, op_options)
    except Exception as e:
        if show_stacktrace:
            job_stacktrace = Common.get_exception_traceback()
            logger.error("Python stacktrace:\n" + job_stacktrace)
        logger.error(str(e))

        # Only re-raise if it's not a UserException
        # if isinstance(e, UserError):
        #     logger.error(str(e))
        # else:
        #     raise e

def main():
    app()

if __name__ == "__main__":
    # sys.argv = ["cli.py", "--help"]

    # Example tests
    # sys.argv = ["cli.py", "run", "bigcommerce_create_customers", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "bigcommerce_fetch_customers", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]

    sys.argv = ["cli.py", "run", "github_repo_health", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "github_fetch_issues", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]

    # sys.argv = ["cli.py", "run", "mailchimp_fetch_subscribers", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "mailchimp_create_segment", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]    

    # sys.argv = ["cli.py", "run", "salesforce_fetch_accounts", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "salesforce_create_accounts", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]

    # sys.argv = ["cli.py", "run", "shopify_fetch_customers", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]

    # sys.argv = ["cli.py", "run", "database_ops_example", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "control_ops_example", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]


    # Utility
    # sys.argv = ["cli.py", "run", "flow1", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "control_ops_example", "--op-id", "set_var1", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc", "--env-dir", "/Users/maximgrinev/sequor-env"]
    # sys.argv = ["cli.py", "run", "mailchimp_create_segment", "--op-id", "add_segment_members", "--debug-foreach-record", '{"email_address":"maxim@paloaltodatabases.com"}', "--debug-request-preview-trace", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor_projects/misc"]


 
    app()