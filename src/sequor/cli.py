# run_flow.py
import logging
import os
from pathlib import Path
import sys
import yaml
from sequor.common import telemetry
from sequor.common.common import Common
from sequor.core.context import Context
from sequor.core.environment import Environment
from sequor.core.execution_stack_entry import ExecutionStackEntry
from sequor.core.instance import Instance
from sequor.core.job import Job
from sequor.core.user_error import UserError
from sequor.operations.run_flow import RunFlowOp
from sequor.project.project import Project

import typer
# import typer.core
# typer.core.rich = None

# Disable rich traceback: rich_traceback=False
app = typer.Typer(
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
    rich_markup_mode=None)
env_app = typer.Typer(
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
    rich_markup_mode=None)
app.add_typer(env_app, name="env")

@app.command()
def version():
    from sequor import __version__
    typer.echo(f"Sequor version: {__version__}")

@env_app.command()
def init(
    env_name: str = typer.Argument(..., help="Name for the new environment."),
    home_dir_cli: str = typer.Option(None, "--home-dir", help="Path to Sequor home directory"),
    show_stacktrace: bool = typer.Option(False, "--stacktrace", help="Show the Python exception stack trace", is_flag=True),
):
    logger = logging.getLogger("sequor.cli")    
    try:
        instance = Instance(home_dir_cli)

        sequor_home_dir = instance.get_home_dir()
        envs_dir = sequor_home_dir / "envs"
        envs_dir.mkdir(parents=True, exist_ok=True)

        env_file = envs_dir / f"{env_name}.yaml"
        if env_file.exists():
            raise UserError(f"Environment with such name already exists: {os.path.abspath(env_file)}")
        env_file.touch()
        env_file.write_text("variables:\n")
        typer.echo(f"Environment initialized successfully: {env_file.resolve()}")        
    except Exception as e:
        if show_stacktrace:
            job_stacktrace = Common.get_exception_traceback()
            logger.error("Python stacktrace:\n" + job_stacktrace)
        logger.error(f"Error initializing environment \"{env_name}\": " + str(e))        
        raise typer.Exit(code=1)

@app.command()
def init(
    project_dir: str = typer.Argument(..., help="Path to directory for the new project. Will be created if it doesn't exist. Example: ~/my-sequor-project"),
    home_dir_cli: str = typer.Option(None, "--home-dir", help="Path to Sequor home directory"),
    show_stacktrace: bool = typer.Option(False, "--stacktrace", help="Show the Python exception stack trace", is_flag=True),
):
    logger = logging.getLogger("sequor.cli")
    try:
        instance = Instance(home_dir_cli)


        # Check if the directory already exists and is not empty
        project_path = Path(os.path.expanduser(project_dir))
        if project_path.exists() and any(project_path.iterdir()):
            raise UserError(f"Project directory '{project_dir}' already exists and is not empty.")
            
        # Create the project directory if it does not exist
        project_path.mkdir(parents=True, exist_ok=True)
        # Initialize the project
        (project_path / "flows").mkdir(parents=True, exist_ok=True)
        (project_path / "sources").mkdir(parents=True, exist_ok=True)
        project_name = project_path.name
        project_conf_file = project_path / f"project.yaml"
        project_conf_file.touch()
        project_conf_file.write_text(f"name: \"{project_name}\"\n")
        logger.info(f"Project initialized successfully at {project_path.resolve()}")
    except Exception as e:
        if show_stacktrace:
            job_stacktrace = Common.get_exception_traceback()
            logger.error("Python stacktrace:\n" + job_stacktrace)
        logger.error(f"Error initializing project \"{project_dir}\": " + str(e))
        raise typer.Exit(code=1)
   

@app.command()
def run(
    flow_name: str = typer.Argument(..., help="Flow to run (e.g. 'myflow' or 'salesforce/account_sync')"),
    # op_mode: str = typer.Option(None, "--op-mode", help="Operation-specific mode for debugging or diagnostics (e.g. 'preview_response' for http_request op)"),
    home_dir_cli: str = typer.Option(None, "--home-dir", help="Path to Sequor home directory"),
    project_dir_cli: str = typer.Option(None, "--project-dir", "-p", help="Path to Sequor project"),
    env_name_cli: str = typer.Option(None, "--env", help="Environment name"),

    # Job-level options
    disable_flow_stacktrace: bool = typer.Option(False, "--disable-flow-stacktrace", help="Show the execution path through the flow operations", is_flag=True),
    show_stacktrace: bool = typer.Option(False, "--stacktrace", help="Show the Python exception stack trace", is_flag=True),

    op_id: str = typer.Option(None, "--op-id", help="ID of the operation to run"),

    # http_request op specific options
    debug_foreach_record: str = typer.Option(None, "--debug-httprequest-foreach-test-record", help="Run with a test for_each record specified as JSON object (or records if batching is enabled as JSON array). The record(s) should match the structure expected by the operation. Example: --debug-test-record='{\"email\":\"test@example.com\"}'"),
    debug_request_preview_trace: bool = typer.Option(False, "--debug-httprequest-preview-trace", help="Run only HTTP request part and show HTTP request trace", is_flag=True),
    debug_request_preview_pretty: bool = typer.Option(False, "--debug-httprequest-preview-pretty", help="Run only HTTP request part and show pretty trace", is_flag=True),
    debug_response_parser_preview: bool = typer.Option(False, "--debug-httprequest-response-parser-preview", help="Show parser result without applying it", is_flag=True),
):
    logger = logging.getLogger("sequor.cli")
    try:
        instance = Instance(home_dir_cli)
        # logger.info("Starting Sequor CLI")
        telemetry_logger = telemetry.getLogger("sequor.cli")
        telemetry_logger.event("cli_start", command="run")
        
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
        env_project_file = project_dir / "env.yaml"
        # default_env_dir = Path.home() / "sequor_env"
        if env_name_cli:
            env_name = env_name_cli
            # env_dir = Path(os.path.expanduser(env_dir_cli))
            # if not env_dir.exists():
            #     raise UserError(f"Environment directory passed as CLI --env-dir argument does not exist: {env_dir_cli}")
        elif env_project_file.exists():
            with env_project_file.open("r") as f:
                env_project_data = yaml.safe_load(f)
                if "env" not in env_project_data:
                    raise UserError(f"'env' key not found in project environment file: {env_project_file}")
                env_name = env_project_data["env"]
                # env_dir = Path(os.path.expanduser(project_env_data["env_dir"]))
                # if not env_dir.exists():
                #     raise UserError(f"Environment directory referenced in project environment file sequor_env.yaml does not exist: {env_dir}")
        elif env_os_var:
            env_name = env_os_var
            # env_dir = Path(os.path.expanduser(env_os_var))
            # if not env_dir.exists():
            #     raise UserError(f"Environment directory passed as SEQUOR_ENV environment variable does not exist: {env_os_var}")
        # elif default_env_dir.exists():
        #     env_dir = default_env_dir
        # else:
        #     raise UserError(f"Environment directory not found. Please specify it using --env-dir argument, SEQUOR_ENV environment variable, or in project sequor_env.ymal.")

        # Initialize an environment
        if env_name is not None:
            env = Environment(env_name, instance.get_home_dir())
            env.load()
        else:
            env = Environment.create_empty(instance.get_home_dir())

        # # Register all operations at program startup
        # register_all_operations()

        # Initialize a project
        project = Project(project_dir, instance.get_home_dir())

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
        job.run(op_options)
    except Exception as e:
        if show_stacktrace:
            job_stacktrace = Common.get_exception_traceback()
            logger.error("Python stacktrace:\n" + job_stacktrace)
        logger.error(str(e))
        raise typer.Exit(code=1)

def main():
    app()

if __name__ == "__main__":
    # sys.argv = ["cli.py", "version"]

    # sys.argv = ["cli.py", "--help"]
    # sys.argv = ["cli.py", "env", "init", "dev", "--home-dir", "/Users/maximgrinev/.sequor-dev"]
    # sys.argv = ["cli.py", "init", "~/myprogs/sequor-misc123", "--home-dir", "/Users/maximgrinev/.sequor-dev"]


    # sequor-integrations tests
    sys.argv = ["cli.py", "run", "0_run_tests", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor-integrations", "--env", "dev", "--home-dir", "/Users/maximgrinev/.sequor-dev"]


    # Utility
    # sys.argv = ["cli.py", "run", "bigcommerce_fetch_customers_light", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor-integrations", "--env", "dev", "--home-dir", "/Users/maximgrinev/.sequor-dev"]
    # sys.argv = ["cli.py", "run", "hubspot_fetch_companies", "--op-id", "get_companies", "--stacktrace", "--project-dir",
    #             "/Users/maximgrinev/myprogs/sequor-integrations", "--env", "dev", "--home-dir", "/Users/maximgrinev/.sequor-dev"]
    # sys.argv = ["cli.py", "run", "hubspot_fetch_companies", "--op-id", "get_companies", "--debug-httprequest-preview-trace", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor-integrations", "--env", "dev", "--home-dir", "/Users/maximgrinev/.sequor-dev"]
    # sys.argv = ["cli.py", "run", "bigcommerce_create_customers", "--op-id", "create_customers", "--debug-httprequest-foreach-test-record", '{"id":"1", "first_name":"Bob", "last_name": "brown", "email":"bob@brown1.net"}', "--debug-httprequest-preview-trace", "--stacktrace", "--project-dir", "/Users/maximgrinev/myprogs/sequor-integrations", "--env-dir", "/Users/maximgrinev/sequor-env"]


 
    app()