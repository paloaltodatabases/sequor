from typing import Any, Dict
from flow.op import Op, OpType


class TransformOp(Op):
    def __init__(self, proj, conf: Dict[str, Any]):
        self.type = OpType.TRANSFORM
        self.proj = proj
        self.conf = conf

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        source_name = self.conf.get('source')
        query = self.conf.get('query')
        target_table = self.conf.get('target_table')

        source = self.proj.get_source(source_name)
        with source.connect() as conn:
            conn.drop_table_if_exists(target_table)
            createTableSql = source.get_create_table_sql(query,target_table)
            print(f"createTableSql: {createTableSql}")
            conn.execute_update(createTableSql)
    
        print(f"TransformOp - done")
        return context