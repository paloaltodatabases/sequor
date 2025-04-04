from datetime import datetime
import time

class ExecutionStackEntry:
    def __init__(self, op_title: str, flow_type_name: str, flow_name: str, flow_step_index: int, flow_step_index_name: str, parent: 'ExecutionStackEntry'):
        self.op_title = op_title
        self.flow_type_name = flow_type_name
        self.flow_name = flow_name
        self.flow_step_index = flow_step_index
        self.flow_step_index_name = flow_step_index_name
        self.parent = parent
        self.start_time = datetime.now()
