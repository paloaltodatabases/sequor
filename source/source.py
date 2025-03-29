from typing import Any, Dict

from source.table_address import TableAddress

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

    def get_qualified_name(self, table_addr: TableAddress):
        raise NotImplementedError("Subclasses must implement get_qualified_name()")

    def get_default_namespace_name(self):
        raise NotImplementedError("Subclasses must implement get_default_namespace_name()")