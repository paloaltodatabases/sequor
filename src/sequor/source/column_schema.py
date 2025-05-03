from sequor.source.data_type import DataType


class ColumnSchema:
    # do we need to add position: int
    def __init__(self, name: str, type: DataType):
        self.name = name
        self.type = type

    
    


