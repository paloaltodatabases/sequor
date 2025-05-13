import os

from pathlib import Path

import yaml

from sequor.core.instance import Instance
from sequor.core.user_error import UserError



class Environment:
    def __init__(self, instance: Instance, env_name: str):
        self.env_name = env_name
        self.instance = instance

        home_dir = instance.get_home_dir()
        env_file = home_dir / "envs" / (env_name + ".yaml")
        if not env_file.exists():
            raise UserError(f"Environment does not exist: file {env_file.resolve()} not found.")
        # Load environment variables in constructor because they cannot be changed
        # env_vars_file = os.path.join(self.env_dir, "variables.yaml")
        # if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            try:
                env_file_data = yaml.safe_load(f) or {}
            except Exception as e:
                raise UserError(f"Error parsing environment file {env_file.resolve()}: {e}")
        self.env_vars = env_file_data.get("variables", {})
   
    @classmethod
    def create_empty(cls, instance: Instance) -> 'Environment':
        env = Environment.__new__(Environment)
        env.env_name = None
        env.instance = instance
        env.env_vars = {}
        return env 

    def get_variable_value(self, var_name: str):
        value = self.env_vars.get(var_name)
        return value
