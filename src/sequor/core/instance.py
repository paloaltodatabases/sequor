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


    def get_home_dir(self) -> Path:
        return self.home_dir
    
    def get_project_state_dir(self) -> Path:
        return self.home_dir / "project_state"
