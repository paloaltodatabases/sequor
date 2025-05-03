import os
from pathlib import Path

import yaml

from sequor.core.user_error import UserError


class Environment:
    def __init__(self, env_dir: Path):
        self.env_dir = env_dir
        # Load environment variables in constructor because they cannot be changed
        env_vars_file = os.path.join(self.env_dir, "variables.yaml")
        if os.path.exists(env_vars_file):
            with open(env_vars_file, 'r') as f:
                try:
                    self.env_vars = yaml.safe_load(f) or {}
                except Exception as e:
                    raise UserError(f"Error loading environment variables from file {env_vars_file}: {e}")
        else:
            self.env_vars = {}

    
    def get_project_state_dir(self, project_name: str) -> Path:
        return self.env_dir / "project_state" / project_name

    def get_variable_value(self, var_name: str):
        return self.env_vars.get(var_name)
