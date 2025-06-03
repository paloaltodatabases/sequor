import os

from pathlib import Path

import yaml


# from sequor.core.instance import Instance
from sequor.core.user_error import UserError



class Environment:
    def __init__(self, env_name: str, home_dir):  # instance: Instance,
        self.env_name = env_name
        self.home_dir = home_dir
        # self.instance = instance


   
    @classmethod
    def create_empty(cls) -> 'Environment': # instance: Instance,
        env = Environment.__new__(Environment)
        env.env_name = None
        # env.instance = instance
        env.env_vars = {}
        return env 

    def load(self):
        env_file = self.home_dir / "envs" / (self.env_name + ".yaml")
        if not env_file.exists():
            raise UserError(f"Environment does not exist: file {env_file.resolve()} not found.")
        with open(env_file, 'r') as f:
            try:
                env_file_data = yaml.safe_load(f) or {}
            except Exception as e:
                raise UserError(f"Error parsing environment file {env_file.resolve()}: {e}")
        self.env_vars = env_file_data.get("variables", {})
        
    def get_variable_value(self, var_name: str):
        value = self.env_vars.get(var_name)
        return value
