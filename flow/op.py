from enum import Enum
from typing import Any, Dict

class OpType(Enum):
    TRANSFORM = "transform"
    

class Op:
    """Base class for all operations"""
    id: str
    type: OpType
    config: Dict[str, Any]
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute this operation with the given context"""
        raise NotImplementedError("Subclasses must implement run")