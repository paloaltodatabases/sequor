from typing import Any, Dict, List
from sequor.source.column_schema import ColumnSchema
from sequor.source.data_type import DataType


class Model:
    def __init__(self):
        self.columns = []

    @classmethod
    def from_columns(cls, columns: List[ColumnSchema]):
        model = cls()
        model.columns = columns
        return model

    @classmethod
    def from_model_def(cls, model_def: Dict[str, Any]):
        columns_def = model_def.get("columns", [])

        columns_def_list = None
        if isinstance(columns_def, dict):
            # Convert compact object notation
            columns_def_list = [
                {"name": name, "type": type_def}
                for name, type_def in columns_def.items()
            ]
        else:
            columns_def_list = columns_def

        # load columns
        columns = []
        for col_def in columns_def_list:
            name = col_def.get("name")
            type = DataType.from_column_def(col_def)
            columns.append(ColumnSchema(name, type))

        return Model.from_columns(columns)