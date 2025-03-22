from enum import Enum
import os
import yaml
from typing import Any, Dict, List

from flow.flow import Flow
from flow.op import Op
from operations.http_request import HttpRequestOp
from operations.transform import TransformOp
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
        self.flows_cache: Dict[str, Flow] = {}
        
        # Ensure flows directory exists
        if not os.path.exists(self.flows_dir):
            raise ValueError(f"Flows directory not found: {self.flows_dir}")

    
    def get_flow(self, flow_name: str) -> Flow:
        """
        Get a flow by name, loading and parsing it from YAML if not cached.
        
        Args:
            flow_name: Name of the flow to retrieve
        
        Returns:
            Flow: The parsed flow object
            
        Raises:
            FileNotFoundError: If the flow file doesn't exist
            ValueError: If the flow definition is invalid
        """
        # Return from cache if already loaded
        if flow_name in self.flows_cache:
            return self.flows_cache[flow_name]
        
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
        
        # Cache the flow for future use
        self.flows_cache[flow_name] = flow
        
        return flow
    
    def _parse_flow(self, flow_name: str, flow_def: Dict[str, Any]) -> Flow:
        """
        Parse a flow definition dictionary into a Flow object.
        
        Args:
            flow_name: Name of the flow
            flow_def: Dictionary containing the flow definition
            
        Returns:
            Flow: The parsed flow object
        """
        # Extract flow metadata
        description = flow_def.get('description', '')
        
        # Create flow object
        flow = Flow(flow_name, description)
        
        # Parse operations
        ops = flow_def.get('ops', [])
        for op_def in ops:
            op = self._parse_op(op_def)
            flow.add_step(op)
        
        return flow
    
    def _parse_op(self, op_def: Dict[str, Any]) -> Op:
        """
        Parse an operation definition into an Op object.
        
        Args:
            op_def: Dictionary containing the operation definition
            
        Returns:
            Op: The parsed operation object
            
        Raises:
            ValueError: If the operation type is unknown
        """
        op_type = op_def.get('op')
        op_id = op_def.get('id', f"{op_type}_{id(op_def)}")
        
        if op_type == 'transform':
            return TransformOp(self, op_def)
        elif op_type == 'http_request':
            return HttpRequestOp(self, op_def)
        else:
            raise ValueError(f"Unknown operation type: {op_type}")
    
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
        source_file = os.path.join(self.project_path, "sources", f"{source_name}.yaml")
        
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
