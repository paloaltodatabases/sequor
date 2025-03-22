from typing import Any, Dict
from flow.op import Op, OpType
import requests


class HttpRequestOp(Op):
    def __init__(self, proj, conf: Dict[str, Any]):
        self.type = OpType.TRANSFORM
        self.proj = proj
        self.conf = conf

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        url = self.conf.get('url')
        method = self.conf.get('method')
        headers = self.conf.get('headers')

        response = requests.request(
            method=method,  # or "POST", "PUT", "DELETE", etc.
            url=url,
            # params={"key": "value"},  # Query parameters
            headers= headers # {"Content-Type": "application/json"},
            # json={"data": "payload"},  # JSON body
            # data={"form": "data"},     # Form data
            # auth=("username", "password"),
            # timeout=10,
            # verify=True,  # SSL verification
        )
        print(f"response: {response.json()}")
        print(f"HttpRequestOp - done")
        return context