class TableAddress:
    def __init__(self, database_name, namespace_name, table_name):
        self.database_name = database_name
        self.namespace_name = namespace_name
        self.table_name = table_name
    def clone(self):
        """
        Create a copy of this TableAddress instance.
        
        Returns:
            TableAddress: A new TableAddress instance with the same values
        """
        return TableAddress(
            database_name=self.database_name,
            namespace_name=self.namespace_name, 
            table_name=self.table_name
        )
