from typing import Any, Dict, Type, ClassVar

class Op:
    """Base class for all operations"""
    name: str
    config: Dict[str, Any]
    
    # Registry to store operation types and their corresponding classes
    _registry: ClassVar[Dict[str, Type['Op']]] = {}
    
    @classmethod
    def register(cls, op_type: str):
        """Decorator to register operation classes"""
        def decorator(op_class: Type['Op']):
            # Register the operation class
            cls._registry[op_type] = op_class
            return op_class
        return decorator
    
    @classmethod
    def create(cls, op_type: str, proj, op_def: Dict[str, Any]) -> 'Op':
        """Factory method to create operation instances"""
        if op_type not in cls._registry:
            raise ValueError(f"Unknown operation type: {op_type}")
        return cls._registry[op_type](proj, op_def)
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute this operation with the given context"""
        raise NotImplementedError("Subclasses must implement run")