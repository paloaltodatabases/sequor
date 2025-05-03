from typing import Any, Dict


class Specification:
    def __init__(self, name: str, type: str, spec_def: Dict[str, Any]):
        self.name = name
        self.type = type
        self.spec_def = spec_def
