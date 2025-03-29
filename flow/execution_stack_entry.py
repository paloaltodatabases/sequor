from datetime import datetime
import time

class ExecutionStackEntry:
    def __init__(self, op_title: str, flow_name: str, op_index: int, parent: 'ExecutionStackEntry'):
        self.op_title = op_title
        self.flow_name = flow_name
        self.op_index = op_index
        self.parent = parent
        self.start_time = datetime.now()
