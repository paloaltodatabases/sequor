from enum import Enum
import os
from pathlib import Path
import tempfile
# import yaml
from ruamel.yaml import YAML

# from sequor.core.environment import Environment
# from sequor.core.instance import Instance
from sequor.core.context import Context
from sequor.core.registry import create_op, create_source
from sequor.operations.block import BlockOp
from sequor.operations.execute import ExecuteOp
from sequor.operations.for_each import ForEachOp
from sequor.operations.http_request import HTTPRequestOp
from sequor.operations.if_op import IfOp
from sequor.operations.print import PrintOp
from sequor.operations.run_flow import RunFlowOp
from sequor.operations.set_variable import SetVariableOp
from sequor.operations.transform import TransformOp
from sequor.source.sources.duckdb_source import DuckDBSource
from typing import Any, Dict, List

from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.core.user_error import UserError
from sequor.project.specification import Specification
from sequor.source.source import Source
from sequor.source.sources.http_source import HTTPSource
from sequor.source.sources.sql_source import SQLSource

class Project:
    def __init__(self, project_dir: Path, home_dir):  # instance: Instance, env: Environment,
        self.yaml = YAML()
        self.yaml.preserve_quotes = True 
        # self.instance = instance

        # self.env = env
        self.home_dir = home_dir
        self.project_dir = project_dir
        self.flows_dir = os.path.join(project_dir, "flows")
        self.sources_dir = os.path.join(project_dir, "sources")
        self.specs_dir = os.path.join(project_dir, "specifications")

        # Load project configuration file
        project_def_file = os.path.join(self.project_dir, f"project.yaml")
        if not os.path.exists(project_def_file):
            raise UserError(f"Project configuration file does not exist: {project_def_file}")

        with open(project_def_file, 'r') as f:
            project_def = self.yaml.load(f)
            self.project_name = project_def.get('name')
            if self.project_name is None:
                raise UserError(f"Project configuration file does not contain 'name' field: {project_def_file}")
            # self.project_version = project_def.get('version')
     
        self.project_state_dir = self.home_dir / "project_state" / self.project_name
        self.project_vars_file = os.path.join(self.project_state_dir, "variables.yaml")
        
    def get_source(self, context: Context, source_name: str) -> Any:
        # Construct flow file path
        source_file = os.path.join(self.sources_dir, f"{source_name}.yaml")

        # Check if file exists
        if not os.path.exists(source_file):
            raise UserError(
                f"Source \"{source_name}\" not found: file does not exist: {source_file}")

        # Load and parse the flow
        with open(source_file, 'r') as f:
            source_def = self.yaml.load(f)
        source = create_source(context, source_name, source_def)
        return source
    
    # @classmethod
    # def create(cls, proj, op_def: Dict[str, Any]) -> 'Op':

    # @staticmethod
    # def op_from_def(proj, op_def: Dict[str, Any]) -> 'Op':
    #     return create_op(proj, op_def)

    def get_flow(self, flow_name: str) -> Flow:
        # Construct flow file path
        flow_file = os.path.join(self.flows_dir, f"{flow_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(flow_file):
            raise UserError(f"Flow \"{flow_name}\" not found: file does not exist: {flow_file}")
        
        # Load and parse the flow
        try:
            with open(flow_file, 'r') as f:
                flow_def = self.yaml.load(f)
        except Exception as e:
            raise UserError(f"Error loading flow definition: {e}")
        
        # Parse the flow definition into a Flow object
        description = flow_def.get('description', '')
        flow = Flow("flow", flow_name, description)
        ops = flow_def.get('steps', [])
        for op_def in ops:
            op = create_op(self, op_def)
            flow.add_step(op)

        return flow
    
    def build_flow_from_block_def(self, block_def: List[Dict[str, Any]]) -> Flow:
        flow = Flow("block", name = None, description = None)
        for op_def in block_def:
            op = create_op(self, op_def)
            flow.add_step(op)
        return flow
    
    
    def list_flows(self) -> List[str]:
        if not os.path.exists(self.flows_dir):
            return []
            
        flow_files = [f for f in os.listdir(self.flows_dir) 
                     if f.endswith('.yaml') and os.path.isfile(os.path.join(self.flows_dir, f))]
        
        # Strip .yaml extension to get flow names
        flow_names = [os.path.splitext(f)[0] for f in flow_files]
        
        return flow_names

    
    def get_specification(self, spec_type: str, spec_name: str) -> Specification:
        # Construct file path
        spec_file = os.path.join(self.specs_dir, spec_type, f"{spec_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(spec_file):
            raise UserError(f"Specification \"{spec_name}\" not found: file does not exist: {spec_file}")
        
        # Load and parse the flow
        with open(spec_file, 'r') as f:
            spec_def = self.yaml.load(f)
        
        return spec_def
    
    def set_variable(self, var_name: str, var_value: Any):
        # Read current data
        try:
            with open(self.project_vars_file, 'r') as f:
                vars = self.yaml.load(f) or {}
        except (FileNotFoundError):  
            # Ensure the directory for the variables file exists. We will create the file later when we write the variable
            project_vars_dir = os.path.dirname(self.project_vars_file)
            os.makedirs(project_vars_dir, exist_ok=True)
            vars = {}
        
        # Update variable
        vars[var_name] = var_value
        
        # Write to temp file and replace
        dir_path = os.path.dirname(self.project_vars_file)
        fd, temp_path = tempfile.mkstemp(dir=dir_path or '.')
        try:
            with os.fdopen(fd, 'w') as f:
                self.yaml.dump(vars, f)
            os.replace(temp_path, self.project_vars_file)
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
    
    def get_variable_value(self, var_name: str):
        # Try to read the variables file
        try:
            with open(self.project_vars_file, 'r') as f:
                vars = self.yaml.load(f) or {}
                
            # Return the variable value if it exists, otherwise return default_value
            return vars.get(var_name) # None if the variable is not set
        except FileNotFoundError:
            # File doesn't exist, means that the variable is not set
            return None
    
