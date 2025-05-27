from typing import Any, Dict, Union


class DataType:
    def __init__(self, name: str, precision: Union[int, None] = None, scale: Union[int, None] = None):
        self.name = name
        self.precision = precision
        self.scale = scale
    
    @classmethod
    def from_column_def(cls, col_def: Dict[str, Any]):
        type_def = col_def.get("type")
        if isinstance(type_def, str):
            dt = {
                "name": type_def,
                "precision": 0,
                "scale": 0
            }
        else:
            dt = {
                "name": type_def.get("name"),
                "precision": type_def.get("precision", 0), 
                "scale": type_def.get("scale", 0)
            }
        return cls(dt["name"], dt["precision"], dt["scale"])

    def __str__(self): 
        return f"{self.name}"