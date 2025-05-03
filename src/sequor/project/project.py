from enum import Enum
import os
from pathlib import Path
import tempfile
# import yaml
from ruamel.yaml import YAML

from sequor.core.environment import Environment
from sequor.source.sources.duckdb_source import DuckDBSource
yaml = YAML()
yaml.preserve_quotes = True
from typing import Any, Dict, List

from sequor.core.flow import Flow
from sequor.core.op import Op
from sequor.core.user_error import UserError
from sequor.project.specification import Specification
from sequor.source.source import Source
from sequor.source.sources.http_source import HTTPSource
from sequor.source.sources.sql_source import SQLSource

class Project:
    def __init__(self, env: Environment, project_dir: Path):
        self.env = env
        self.project_dir = project_dir
        self.flows_dir = os.path.join(project_dir, "flows")
        self.sources_dir = os.path.join(project_dir, "sources")
        self.specs_dir = os.path.join(project_dir, "specifications")

        # Load project configuration file
        project_def_file = os.path.join(self.project_dir, f"sequor_project.yaml")
        if not os.path.exists(project_def_file):
            raise UserError(f"Project configuration file does not exist: {project_def_file}")

        with open(project_def_file, 'r') as f:
            project_def = yaml.load(f)
            
        self.project_name = project_def.get('name')
        if self.project_name is None:
            raise UserError(f"Project configuration file does not contain 'name' field: {project_def_file}")
        self.project_version = project_def.get('version')

        self.project_state_dir = env.get_project_state_dir(self.project_name)
        self.project_vars_file = os.path.join(self.project_state_dir, "variables.yaml")
        

    def get_flow(self, flow_name: str) -> Flow:
        # Construct flow file path
        flow_file = os.path.join(self.flows_dir, f"{flow_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(flow_file):
            raise UserError(f"Flow \"{flow_name}\" not found: file does not exist: {flow_file}")
        
        # Load and parse the flow
        try:
            with open(flow_file, 'r') as f:
                flow_def = yaml.load(f)
        except Exception as e:
            raise UserError(f"Error loading flow definition: {e}")
        
        # Parse the flow definition into a Flow object
        description = flow_def.get('description', '')
        flow = Flow("flow", flow_name, description)
        ops = flow_def.get('steps', [])
        for op_def in ops:
            op = Op.op_from_def(self, op_def)
            flow.add_step(op)

        return flow
    
    def build_flow_from_block_def(self, block_def: List[Dict[str, Any]]) -> Flow:
        flow = Flow("block", name = None, description = None)
        for op_def in block_def:
            op = Op.op_from_def(self, op_def)
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

    def get_source(self, source_name: str) -> Any:
        # Construct flow file path
        source_file = os.path.join(self.sources_dir, f"{source_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(source_file):
            raise UserError(f"Source \"{source_name}\" not found: file does not exist: {source_file}")
        
        # Load and parse the flow
        with open(source_file, 'r') as f:
            source_def = yaml.load(f)
        
        source: Source = None
        source_type = source_def.get('type')
        if source_type == 'http':
            source = HTTPSource(self, source_name, source_def)        
        elif source_type == 'postgres':
            source = SQLSource(self, source_name, source_def)
        elif source_type == 'duckdb':
            source = DuckDBSource(self, source_name, source_def)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        return source
    
    def get_specification(self, spec_type: str, spec_name: str) -> Specification:
        # Construct file path
        spec_file = os.path.join(self.specs_dir, spec_type, f"{spec_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(spec_file):
            raise UserError(f"Specification \"{spec_name}\" not found: file does not exist: {spec_file}")
        
        # Load and parse the flow
        with open(spec_file, 'r') as f:
            spec_def = yaml.load(f)
        
        return spec_def
    
    def set_variable(self, var_name: str, var_value: Any):
        # Read current data
        try:
            with open(self.project_vars_file, 'r') as f:
                vars = yaml.load(f) or {}
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
                yaml.dump(vars, f)
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
                vars = yaml.load(f) or {}
                
            # Return the variable value if it exists, otherwise return default_value
            return vars.get(var_name) # None if the variable is not set
        except FileNotFoundError:
            # File doesn't exist, means that the variable is not set
            return None
    
