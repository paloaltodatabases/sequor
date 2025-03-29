import json
from typing import Any, Dict, List
from common.data_loader import DataLoader
from flow.op import Op
import requests
from common.executor_utils import load_user_function
from source.row import Row
from source.table_address import TableAddress


class HTTPRequestParameters:
    def __init__(self, url, method, parameters, headers, body, parse_response_fun):
        self.url = url
        self.method = method
        self.parameters = parameters
        self.headers = headers
        self.body = body
        self.parse_response_fun = parse_response_fun


@Op.register('http_request')
class HTTPRequestOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def _make_request_helper(self, http_params: HTTPRequestParameters, input_rows: List[Row]):
        json = http_params.body(input_rows) if http_params.body and callable(http_params.body) else http_params.body
        response = requests.request(
            method = http_params.method(input_rows) if callable(http_params.method) else http_params.method,  # or "POST", "PUT", "DELETE", etc.
            url = http_params.url(input_rows) if callable(http_params.url) else http_params.url,
            params = http_params.parameters(input_rows) if callable(http_params.parameters) else http_params.parameters, # {"key": "value"},  # Query parameters
            headers = http_params.headers(input_rows) if callable(http_params.headers) else http_params.headers, # {"Content-Type": "application/json"},
            # json={"data": "payload"},  # JSON body
            # data={"form": "data"},     # Form data
            # auth=("username", "password"),
            # timeout=10,
            # verify=True,  # SSL verification
            json = json
        )
        return response

    def _make_request(self, http_req_params: HTTPRequestParameters, input_rows):
        response = self._make_request_helper(http_req_params, input_rows)
        
        # Print response details for debugging
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
        print(f"Response text: {response.text}")
        
        try:
            response_parsed = http_req_params.parse_response_fun(response, None)
        except Exception as e:
            import traceback
            tb = traceback.extract_tb(e.__traceback__)
            if tb:
                # Find the first frame that's in our code (not in a library)
                for frame in tb:
                    if frame.filename.startswith('<') or frame.filename.endswith('.py'):
                        print(f"#################Error in parser at line {frame.lineno}: {str(e)}")
                        print(f"File: {frame.filename}")
                        if hasattr(http_req_params.parse_response_fun, '__source_code__'):
                            source_code = http_req_params.parse_response_fun.__source_code__.split('\n')
                            if frame.lineno <= len(source_code):
                                print(f"Line: {source_code[frame.lineno - 1]}")
                        break
            raise e

        print(f"response parsed:")
        print(json.dumps(response_parsed, indent=2))

        # data_loader = DataLoader(self.proj, self.http_source, self.source_name, self.table_addr)
        tables_def = response_parsed.get('tables', [])
        self.data_loader.run(tables_def)

    def run(self, context):
        # Extract input def
        input_def = self.op_def.get('input')
        source_name = input_def.get('source')
        database_name = input_def.get('database')
        namespace_name = input_def.get('namespace')
        table_name = input_def.get('table')
        input_table_addr = TableAddress(database_name, namespace_name, table_name)
        # Extract request def
        request_def = self.op_def.get('request')
        url = request_def.get('url')
        url_expression = request_def.get('url_expression')
        if url and url_expression:
            raise Exception("Both url and url_expression are specified in request definition. Only one of them can be specified.")
        elif url_expression:
            url = load_user_function(url_expression, "url_expression")
        elif not url:
            raise Exception("url or url_expression must be specified in request definition.")
        method = request_def.get('method')
        method_expression = request_def.get('method_expression')
        if method and method_expression:
            raise Exception("Both method and method_expression are specified in request definition. Only one of them can be specified.")
        elif method_expression:
            method = load_user_function(method_expression, "method_expression")
        elif not method:
            raise Exception("method or method_expression must be specified in request definition.")
        parameters = request_def.get('parameters')
        parameters_expression = request_def.get('parameters_expression')
        if parameters and parameters_expression:
            raise Exception("Both parameters and parameters_expression are specified in request definition. Only one of them can be specified.")
        elif parameters_expression:
            parameters = load_user_function(parameters_expression, "parameters_expression")
        elif not parameters:
            parameters = {}
        headers = request_def.get('headers')
        headers_expression = request_def.get('headers_expression')
        if headers and headers_expression:
            raise Exception("Both headers and headers_expression are specified in request definition. Only one of them can be specified.")
        elif headers_expression:
            headers = load_user_function(headers_expression, "headers_expression")
        elif not headers:
            headers = {}
        body = request_def.get('body')
        body_expression = request_def.get('body_expression')
        if body and body_expression:
            raise Exception("Both body and body_expression are specified in request definition. Only one of them can be specified.")
        elif body_expression:
            body = load_user_function(body_expression, "body_expression")
        else:
            body = {}
        # Extract response def
        response_def = self.op_def.get('response', {})
        target_source_name = response_def.get('source')
        target_database_name = response_def.get('database')
        target_namespace_name = response_def.get('namespace')
        target_table_name = response_def.get('table')
        target_table_addr = TableAddress(target_database_name, target_namespace_name, target_table_name)
        # Compile parser response function code
        parser = response_def.get('parser')
        if parser:
            raise Exception("parser is not supported. Use parser_expression instead.")
        parse_response_expression = response_def.get('parser_expression')
        parse_response_fun = load_user_function(parse_response_expression, "parser_expression")

        http_req_params = HTTPRequestParameters(url, method, parameters, headers, body, parse_response_fun)
        self.data_loader = DataLoader(self.proj, target_source_name, target_table_addr)

        try:
            if table_name is None:
                self._make_request(http_req_params, [])
            else:
                self.source = self.proj.get_source(source_name)
                with self.source.connect() as conn:
                    conn.open_table_for_read(input_table_addr)
                    i = 0
                    row = conn.next_row()
                    while row is not None:
                        i += 1
                        self._make_request(http_req_params, [row])
                        row = conn.next_row()
        finally:
            self.data_loader.close()

        print(f"http_request - done")