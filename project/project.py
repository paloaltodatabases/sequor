from enum import Enum
import os
from pathlib import Path
import yaml
from typing import Any, Dict, List

from flow.flow import Flow
from flow.op import Op
from flow.user_error import UserError
from project.specification import Specification
from source.source import Source
from source.sources.http_source import HTTPSource
from source.sources.sql_source import SQLSource

class Project:
    id: str
    config: Dict[str, Any]

    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.flows_dir = os.path.join(project_dir, "flows")
        self.sources_dir = os.path.join(project_dir, "sources")
        self.specs_dir = os.path.join(project_dir, "specifications")

        # Load project configuration file
        project_def_file = os.path.join(self.project_dir, f"sequor_project.yaml")
        if not os.path.exists(project_def_file):
            raise UserError(f"Project configuration file does not exist: {project_def_file}")

        with open(project_def_file, 'r') as f:
            project_def = yaml.safe_load(f)
            
        self.project_name = project_def.get('name')
        if self.project_name is None:
            raise UserError(f"Project configuration file does not contain 'name' field: {project_def_file}")
        self.project_version = project_def.get('version')

    def get_flow(self, flow_name: str) -> Flow:
        # Construct flow file path
        flow_file = os.path.join(self.flows_dir, f"{flow_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(flow_file):
            raise UserError(f"Flow \"{flow_name}\" does not exist: cannot find file: {flow_file}")
        
        # Load and parse the flow
        with open(flow_file, 'r') as f:
            flow_def = yaml.safe_load(f)
        
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
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        # Load and parse the flow
        with open(source_file, 'r') as f:
            source_def = yaml.safe_load(f)
        
        source: Source = None
        source_type = source_def.get('type')
        if source_type == 'postgres':
            source = SQLSource(source_name, source_def)
        elif source_type == 'http':
            source = HTTPSource(source_name, source_def)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        return source
    
    def get_specification(self, spec_type: str, spec_name: str) -> Specification:
        # Construct file path
        spec_file = os.path.join(self.specs_dir, spec_type, f"{spec_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(spec_file):
            raise FileNotFoundError(f"Specification file not found: {spec_file}")
        
        # Load and parse the flow
        with open(spec_file, 'r') as f:
            spec_def = yaml.safe_load(f)
        
        return spec_def
    
