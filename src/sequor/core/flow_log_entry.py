from datetime import datetime
import logging


logger = logging.getLogger("sequor.job")


class FlowLogEntry:
    def __init__(self, message: str, start_time: datetime, end_time: datetime):
        self.message = message
        self.start_time = start_time
        self.end_time = end_time


    def to_dict(self) -> dict:
        d = {}
        d['message'] = self.message
        d['start_time'] = self.start_time
        d['end_time'] = self.end_time
        return d

 
