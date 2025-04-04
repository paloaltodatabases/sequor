import json
import logging
from typing import Any, Dict, List
from flow.op import Op
import requests
from common.executor_utils import load_user_function, render_jinja
from common.data_loader import DataLoader
from flow.user_error import UserError
from source.row import Row
from source.source import Source
from source.table_address import TableAddress
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests.auth import AuthBase
from requests_toolbelt.utils import dump


class HTTPRequestParameters:
    def __init__(self, auth_handler, url, method, parameters, headers, body, parse_response_fun):
        self.auth_handler = auth_handler
        self.url = url
        self.method = method
        self.parameters = parameters
        self.headers = headers
        self.body = body
        self.parse_response_fun = parse_response_fun


class APIKeyAuth(AuthBase):
    def __init__(self, key, value, add_to='header'):
        self.key = key
        self.value = value
        self.add_to = add_to

    def __call__(self, r):
        if self.add_to == 'header':
            r.headers[self.key] = self.value
        elif self.add_to == 'query':
            from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
            parsed = urlparse(r.url)
            query = dict(parse_qsl(parsed.query))
            query[self.key] = self.value
            r.url = urlunparse(parsed._replace(query=urlencode(query)))
        else:
            raise ValueError(f"Unsupported add_to location: {self.add_to}")
        return r

@Op.register('http_request')
class HTTPRequestOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        super().__init__(proj, op_def)
    
    def get_title(self) -> str:
        request_def = self.op_def.get('request')
        if request_def:
            url = request_def.get('url')
            if url:
                url_title = self.name + ": " + url
        op_title = self.op_def.get('title')
        if (op_title is not None):
            title = self.name + ": " + op_title
        elif url_title:
            title = self.name + ": " + url_title
        else:
            title = self.name + ": unknown"
        return title

    def _make_request_helper(self, http_params: HTTPRequestParameters, input_rows: List[Row], mode: str, logger: logging.Logger):
        # https://requests.readthedocs.io/en/latest/
        json = http_params.body(input_rows) if http_params.body and callable(http_params.body) else http_params.body
        response = requests.request(
            method = http_params.method(input_rows) if callable(http_params.method) else http_params.method,  # or "POST", "PUT", "DELETE", etc.
            url = http_params.url(input_rows) if callable(http_params.url) else http_params.url,
            params = http_params.parameters(input_rows) if callable(http_params.parameters) else http_params.parameters, # {"key": "value"},  # Query parameters
            headers = http_params.headers(input_rows) if callable(http_params.headers) else http_params.headers, # {"Content-Type": "application/json"},
            # json={"data": "payload"},  # JSON body
            # data={"form": "data"},     # Form data
            auth = http_params.auth_handler,
            # timeout=10,
            # verify=True,  # SSL verification
            json = json
        )
        if mode == "preview_response":
            # http_log = dump.dump_all(response, request_prefix=b'>> ', response_prefix=b'<< ')
            http_log = dump.dump_all(response, request_prefix=b'', response_prefix=b'')
            http_log_st = http_log.decode("utf-8")
            logger.info(f"HTTP request trace:\n----------------- TRACE START -----------------\n{http_log_st}\n----------------- TRACE END -----------------")
        return response

    def _make_request(self, http_req_params: HTTPRequestParameters, input_rows, mode: str, logger: logging.Logger):
        response = self._make_request_helper(http_req_params, input_rows, mode, logger)
        
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

        # data_loader = DataLoader(self.proj, self.http_source, self.source_name, self.table_addr)
        tables_def = response_parsed.get('tables', [])
        self.data_loader.run(tables_def)

    def run(self, context, mode: str):
        logger = logging.getLogger("sequor.ops.http_request")
        self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting")

        if mode == "preview_response":
            logger.info("preview_response mode enabled for HTTP request operation")
        else:
            mode = "run"

        # Extract input def
        input_def = self.op_def.get('input')
        if input_def:
            source_name = Op.get_parameter(context, input_def, 'source', is_required=True)
            database_name = Op.get_parameter(context, input_def, 'database', is_required=False)
            namespace_name = Op.get_parameter(context, input_def, 'namespace', is_required=False)
            table_name = Op.get_parameter(context, input_def, 'table', is_required=True)
            input_table_addr = TableAddress(database_name, namespace_name, table_name)
        else:
            input_table_addr = None

        # Extract request def
        request_def = self.op_def.get('request')
        http_source_name = Op.get_parameter(context, request_def, 'source', is_required=False)
        url = Op.get_parameter(context, request_def, 'url', is_required=True)
        method = Op.get_parameter(context, request_def, 'method', is_required=True)
        parameters = Op.get_parameter(context, request_def, 'parameters', is_required=False)
        headers = Op.get_parameter(context, request_def, 'headers', is_required=False)
        body = Op.get_parameter(context, request_def, 'body', is_required=False)

        # Extract response def
        response_def = self.op_def.get('response', {})
        target_source_name = Op.get_parameter(context, response_def, 'source', is_required=False)
        target_database_name = Op.get_parameter(context, response_def, 'database', is_required=False)
        target_namespace_name = Op.get_parameter(context, response_def, 'namespace', is_required=False)
        target_table_name = Op.get_parameter(context, response_def, 'table', is_required=False)
        target_table_addr = TableAddress(target_database_name, target_namespace_name, target_table_name)
        # Compile parser response function code
        parser = response_def.get('parser')
        if parser:
            raise Exception("parser is not supported. Use parser_expression instead.")
        parse_response_expression = response_def.get('parser_expression')
        parse_response_fun = load_user_function(parse_response_expression, "parser_expression")

        auth_handler = None
        http_source = self.proj.get_source(http_source_name)
        http_source_auth_def = Source.get_parameter(context, http_source.source_def, 'auth')
        http_source_auth_type = Source.get_parameter(context, http_source_auth_def, 'type', is_required=True)
        if http_source_auth_type == 'basic_auth':
            http_source_auth_username = Source.get_parameter(context, http_source_auth_def, 'username')
            http_source_auth_password = Source.get_parameter(context, http_source_auth_def, 'password')
            auth_handler = HTTPBasicAuth(http_source_auth_username, http_source_auth_password)
        elif http_source_auth_type == 'bearer_token':
            http_source_auth_token = Source.get_parameter(context, http_source_auth_def, 'token')
            raise UserError("bearer_token auth is not supported yet.")
        elif http_source_auth_type == 'digest_auth':
            http_source_auth_username = Source.get_parameter(context, http_source_auth_def, 'username')
            http_source_auth_password = Source.get_parameter(context, http_source_auth_def, 'password')
            auth_handler = HTTPDigestAuth(http_source_auth_username, http_source_auth_password)
        elif http_source_auth_type == 'api_key':
            http_source_auth_key_name = Source.get_parameter(context, http_source_auth_def, 'key_name')
            http_source_auth_key_value = Source.get_parameter(context, http_source_auth_def, 'key_value')
            http_source_auth_add_to = Source.get_parameter(context, http_source_auth_def, 'add_to')
            auth_handler = APIKeyAuth(http_source_auth_key_name, http_source_auth_key_value, http_source_auth_add_to)
        elif http_source_auth_type == 'oauth_1_0':
            raise UserError("oauth_1_0 auth is not supported yet.")
        elif http_source_auth_type == 'oauth_2_0':
            raise UserError("oauth_2_0 auth is not supported yet.")
        else:
            raise UserError(f"Unsupported auth type: {http_source_auth_type}")

        http_req_params = HTTPRequestParameters(auth_handler, url, method, parameters, headers, body, parse_response_fun)

        if mode == "preview_response":
                self._make_request_helper(http_req_params, [], mode, logger)
        elif mode == "run":
            self.data_loader = DataLoader(self.proj, target_source_name, target_table_addr)
            try:
                if input_table_addr is None:
                    self._make_request(http_req_params, [], mode, logger)
                else:
                    self.source = self.proj.get_source(source_name)
                    with self.source.connect() as conn:
                        conn.open_table_for_read(input_table_addr)
                        i = 0
                        row = conn.next_row()
                        while row is not None:
                            i += 1
                            self._make_request(http_req_params, [row], mode, logger)
                            row = conn.next_row()
            finally:
                self.data_loader.close()

        logger.info(f"Finished")