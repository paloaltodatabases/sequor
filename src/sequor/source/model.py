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
        columnsDef = model_def.get("columns", [])

        # load columns
        columns = []
        for col_def in columnsDef:
            name = col_def.get("name")
            type = DataType.from_column_def(col_def)
            columns.append(ColumnSchema(name, type))

        return Model.from_columns(columns)