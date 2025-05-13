import logging
import os
from sequor.common import telemetry
from pathlib import Path

import yaml

from sequor.core.user_error import UserError


class Instance:
    def __init__(self, home_dir_cli: str):
        # Setting home dir of the Sequor installation
        if home_dir_cli:
            home_dir = Path(os.path.expanduser(home_dir_cli))
        else:
            # Default home dir
            default_home_dir = '~/.sequor'
            home_dir = Path(os.path.expanduser(default_home_dir))

        self.home_dir = home_dir
        # Create home directory if it does not exist
        home_dir.mkdir(parents=True, exist_ok=True)

        # Init logging
        log_dir = self.home_dir / "logs"
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

        self.project_state_dir = self.home_dir / "project_state"

        # Set up telemetry
        telemetry.basicConfig(
            api_key = "phc_XBYG9x8aUaBlQGhNhRwEwJbQ9xCzWs05Cy671pzjxvs", 
            host = "https://us.i.posthog.com", 
            user_id_file = self.home_dir / ".sequor_user_id")


        # # Initialize the instance
        # instance = Instance(home_dir)

        # envs_dir = self.home_dir / "project_state"
        # log_dir = self.home_dir / "logs"

        # # Load environment variables in constructor because they cannot be changed
        # env_vars_file = os.path.join(self.env_dir, "variables.yaml")
        # if os.path.exists(env_vars_file):
        #     with open(env_vars_file, 'r') as f:
        #         try:
        #             self.env_vars = yaml.safe_load(f) or {}
        #         except Exception as e:
        #             raise UserError(f"Error loading environment variables from file {env_vars_file}: {e}")
        # else:
        #     self.env_vars = {}

    
    # def get_project_state_dir(self, project_name: str) -> Path:
    #     return self.env_dir / "project_state" / project_name

    # def get_variable_value(self, var_name: str):
    #     return self.env_vars.get(var_name)
    def get_home_dir(self) -> Path:
        return self.home_dir
    
    def get_project_state_dir(self) -> Path:
        return self.home_dir / "project_state"
