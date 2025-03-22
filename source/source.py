from typing import Any, Dict

class Source:
    """Class representing a data source"""
    def __init__(self, name: str, conf: Dict[str, Any]):
        """
        Initialize a source with a name.
        
        Args:
            name: Name of the source
        """
        self.name = name 
        self.conf = conf

    def connect(self):
        raise NotImplementedError("Subclasses must implement connect()")
