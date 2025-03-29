from enum import Enum
import os
import yaml
from typing import Any, Dict, List

from flow.flow import Flow
from flow.op import Op
from operations.http_request import HTTPRequestOp
from operations.transform import TransformOp
from operations.print import PrintOp
from operations.set_variable import SetVariableOp
from operations.run_flow import RunFlowOp
from project.specification import Specification
from source.sources.sql_source import SQLSource

class Project:
    """class for project operations"""
    id: str
    config: Dict[str, Any]

    def __init__(self, project_path: str):
        """
        Initialize a project from a directory path.
        
        Args:
            project_path: Path to the project directory
        """
        self.project_path = project_path
        self.project_name = os.path.basename(os.path.normpath(project_path))
        self.flows_dir = os.path.join(project_path, "flows")
        self.sources_dir = os.path.join(project_path, "sources")
        self.specs_dir = os.path.join(project_path, "specifications")

        # Ensure flows directory exists
        if not os.path.exists(self.flows_dir):
            raise ValueError(f"Flows directory not found: {self.flows_dir}")

    
    def get_flow(self, flow_name: str) -> Flow:
        # Construct flow file path
        flow_file = os.path.join(self.flows_dir, f"{flow_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(flow_file):
            raise FileNotFoundError(f"Flow file not found: {flow_file}")
        
        # Load and parse the flow
        with open(flow_file, 'r') as f:
            flow_def = yaml.safe_load(f)
        
        # Parse the flow definition into a Flow object
        flow = self._parse_flow(flow_name, flow_def)
        
        return flow
    
    def _parse_flow(self, flow_name: str, flow_def: Dict[str, Any]) -> Flow:
        # Extract flow metadata
        description = flow_def.get('description', '')
        
        # Create flow object
        flow = Flow(flow_name, description)
        
        # Parse operations
        ops = flow_def.get('steps', [])
        for op_def in ops:
            op = self._parse_op(op_def)
            flow.add_step(op)
        
        return flow
    
    def _parse_op(self, op_def: Dict[str, Any]) -> Op:
        op_type = op_def.get('op')
        op_id = op_def.get('id', f"{op_type}_{id(op_def)}")
        
        return Op.create(op_type, self, op_def)
    
    def list_flows(self) -> List[str]:
        """
        List all available flows in the project.
        
        Returns:
            List[str]: List of flow names (without .yaml extension)
        """
        if not os.path.exists(self.flows_dir):
            return []
            
        flow_files = [f for f in os.listdir(self.flows_dir) 
                     if f.endswith('.yaml') and os.path.isfile(os.path.join(self.flows_dir, f))]
        
        # Strip .yaml extension to get flow names
        flow_names = [os.path.splitext(f)[0] for f in flow_files]
        
        return flow_names

    def get_source(self, source_name: str) -> Any:
        """
        Get a source by name.
        
        Args:
            source_name: Name of the source to retrieve
            
        Returns:
            Any: The source object
            
        Raises:
            FileNotFoundError: If the source doesn't exist
        """
        # Construct flow file path
        source_file = os.path.join(self.sources_dir, f"{source_name}.yaml")
        
        # Check if file exists
        if not os.path.exists(source_file):
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        # Load and parse the flow
        with open(source_file, 'r') as f:
            source_def = yaml.safe_load(f)
        
        source_type = source_def.get('type')
        if source_type == 'postgres':
            return SQLSource(source_name, source_def)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        return source_def
    
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
    
