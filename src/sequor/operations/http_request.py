from collections import OrderedDict
import json
import logging
from typing import Any, Dict, List, NamedTuple

import urllib.parse
from sequor.common.common import Common
from sequor.core.context import Context
from sequor.core.op import Op
import requests
from sequor.common.executor_utils import UserContext, UserFunction, load_user_function, render_jinja, set_variable_from_def
from sequor.common.data_loader import DataLoader
from sequor.core.user_error import UserError
from sequor.source.row import Row
from sequor.source.source import Source
from sequor.source.table_address import TableAddress
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests.auth import AuthBase
from requests_toolbelt.utils import dump
from authlib.integrations.requests_client import OAuth2Session


class HTTPRequestParameters:
    def __init__(self, auth_handler, oauth_session, url, method, parameters, headers, body_format, body, parse_response_fun):
        self.auth_handler = auth_handler
        self.oauth_session = oauth_session
        self.url = url
        self.method = method
        self.parameters = parameters
        self.headers = headers
        self.body = body
        self.body_format = body_format
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

class BearerTokenAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

class OAuth2PasswordFlowSession():
    def __init__(self, authlib_session: OAuth2Session, token_endpoint, client_id, client_secret, username, password, **kwargs):
        self.authlib_session = authlib_session
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.token = None
            
    def ensure_active_token(self):
        """Check if token exists and is valid, fetch or refresh as needed"""
        if self.token is None:
            self.token = self.authlib_session.fetch_token(
                self.token_endpoint,
                grant_type='password',
                username=self.username,
                password=self.password,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        elif self.token.is_expired():
            self.refresh_token(self.token_endpoint)
        return self.token

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
            else:
                url_title = None
        else:
            url_title = None
        op_title = self.op_def.get('title')
        op_id = self.op_def.get('id')
        if op_id:
            title = self.name + ": " + op_id
        elif op_title is not None:
            title = self.name + ": " + op_title
        elif url_title:
            title = self.name + ": " + url_title
        else:
            title = self.name + ": unknown"
        return title



    def _convert_yaml_to_python(self, obj):
        """Recursively convert YAML objects (CommentedKeyMap, etc.) to Python types"""
        if isinstance(obj, OrderedDict) or hasattr(obj, 'items'):
            return dict({str(k): self._convert_yaml_to_python(v) for k, v in obj.items()})
        elif isinstance(obj, (list, tuple)):
            return [self._convert_yaml_to_python(v) for v in obj]
        return obj

    def _make_request_helper(self, context: Context, http_params: HTTPRequestParameters, op_options: Dict[str, Any], logger: logging.Logger):
        # Requests lib docs: https://requests.readthedocs.io/en/latest/
        http_service = None
        auth_handler = None
        if http_params.oauth_session:
            http_service = http_params.oauth_session.authlib_session
            http_params.oauth_session.ensure_active_token()
            auth_handler = None
        else:
            http_service = requests
            auth_handler = http_params.auth_handler

        # Serialize body to body_format
        body = Op.eval_parameter(context, http_params.body, render=1, null_literal=True)
        request_body = None
        if http_params.body_format == "json":
            body_dict = self._convert_yaml_to_python(body)
            body_test = {
                "email_address": "test@test.com"
            }
            request_body = json.dumps(self._convert_yaml_to_python(body))
            # if "Content-Type" not in headers:
            # #     headers["Content-Type"] = "application/json"
        if http_params.body_format == "form_urlencoded":
            request_body = urllib.parse.urlencode(self._convert_yaml_to_python(body))
            # if "Content-Type" not in headers:
            #     headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif http_params.body_format == "multipart_form_data":
            raise UserError("multipart_form_data is not supported yet")
            # # Handle files vs regular data
            # request_files = {}
            # request_data = {}
            # # Separate file fields from regular data fields
            # for key, value in body.items():
            #     if isinstance(value, str) and value.startswith("file://"):
            #         file_path = value[7:]  # Remove file:// prefix
            #         request_files[key] = open(file_path, "rb")
            #     else:
            #         request_data[key] = value
        elif http_params.body_format == "xml":
            # Assume body is already XML string or convert dict to XML
            request_body = self._convert_yaml_to_python(body)
            # if "Content-Type" not in headers:
            #     headers["Content-Type"] = "application/xml"
        elif http_params.body_format == "text":
            request_body = self._convert_yaml_to_python(body)
            # if "Content-Type" not in headers:
            #     headers["Content-Type"] = "text/plain"
        elif http_params.body_format == "binary":
            raise UserError("multipart_form_data is not supported yet")
            # Assume body is either binary data or a path to a file
            if isinstance(body, str) and body.startswith("file://"):
                with open(body[7:], "rb") as f:
                    request_body = f.read()
            else:
                request_body = self._convert_yaml_to_python(body)
            if "Content-Type" not in headers:
                headers["Content-Type"] = "application/octet-stream"

        response = http_service.request(
            method = Op.eval_parameter(context, http_params.method, render=1),  # or "POST", "PUT", "DELETE", etc.
            url = Op.eval_parameter(context, http_params.url, render=1),
            params = Op.eval_parameter(context, http_params.parameters, render=1), # {"key": "value"},  # Query parameters
            headers = Op.eval_parameter(context, http_params.headers, render=1), # {"Content-Type": "application/json"},
            auth = auth_handler,
            # json={"data": "payload"},  # JSON body
            # data={"form": "data"},     # Form data
            data = request_body
            # timeout=10,
            # verify=True,  # SSL verification
        )
        if op_options.get("debug_request_preview_trace"):
            # http_log = dump.dump_all(response, request_prefix=b'>> ', response_prefix=b'<< ')
            http_log = dump.dump_all(response, request_prefix=b'', response_prefix=b'')
            http_log_st = http_log.decode("utf-8")
            logger.info(f"HTTP request trace:\n----------------- TRACE START -----------------\n{http_log_st}\n----------------- TRACE END -----------------")
        return response

    def _make_request(self, context, http_req_params: HTTPRequestParameters, op_options: Dict[str, Any], logger: logging.Logger):
        while True:
            response = self._make_request_helper(context, http_req_params, op_options, logger)
            
            response_parsed = http_req_params.parse_response_fun.apply(UserContext(context),response)

            # set returned variables
            for name, value_def in response_parsed.get('variables', {}).items():
                set_variable_from_def(context, name, value_def)

            # load returned tables
            tables_def = response_parsed.get('tables', [])
            self.data_loader.run(tables_def)

            while_def = response_parsed.get('while', [])
            if while_def:
                if not isinstance(while_def, bool):
                    raise UserError("\"while\" in the result of the response parser must be a boolean")
                if not while_def:
                    break
            else:
                break


    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.http_request")
        logger.info(f"Starting \"" + self.get_title() + "\"")

        # clone context because we extend it with source variables and for_each variable that we don't want to be passed to the next op
        parent_context = context
        context = context.clone()

        # extend context with source variables
        request_def = Op.get_parameter(context, self.op_def, 'request', is_required=True)
        http_source_name = Op.get_parameter(context, request_def, 'source', is_required=False) # at this point context is equal to parent_context which is what we want
        if http_source_name:
            http_source = self.proj.get_source(http_source_name)
            variables_def = http_source.source_def.get("variables");
            if variables_def:
                for var_name, var_value in variables_def.items():
                    var_value = render_jinja(context, var_value) # at this point context is equal to parent_context which is what we want
                    context.set_variable(var_name, var_value)
        
        # render http source def in the context extended with source variable 
        # because source properties can contain references to the variables
        if http_source_name:
            http_source_def = http_source.get_rendered_def(context)
        # self.op_def = render_jinja(context, self.op_def)

        # Extract input def (rendered)
        foreach_def = self.op_def.get('for_each')
        if foreach_def:
            location_desc="for_each section"
            # do not render the rest of foreach_def here as it can cause variable unresolved error in case of using --debug_ parameters
            def parse_foreach_def():
                nonlocal location_desc
                foreach_source_name = Op.get_parameter(context, foreach_def, 'source', is_required=True, render=3, location_desc=location_desc)
                foreach_database_name = Op.get_parameter(context, foreach_def, 'database', is_required=False, render=3)
                foreach_namespace_name = Op.get_parameter(context, foreach_def, 'namespace', is_required=False, render=3)
                foreach_table_name = Op.get_parameter(context, foreach_def, 'table', is_required=True, render=3, location_desc=location_desc)
                foreach_table_addr = TableAddress(foreach_database_name, foreach_namespace_name, foreach_table_name)
                return foreach_source_name, foreach_table_addr
            foreach_var_name = Op.get_parameter(context, foreach_def, 'as', is_required=True, render=3, location_desc=location_desc)
        else:
            foreach_table_addr = None

        # Extract request def (render only _expression parameters - non-expression parameters will be rendered on each iteration)
        # request_def = Op.get_parameter(context, self.op_def, 'request', is_required=True) # get request_def again as we need it to be rendered in the context extended with source variables
        url = Op.get_parameter(context, request_def, 'url', is_required=True, render=2)
        method = Op.get_parameter(context, request_def, 'method', is_required=True, render=2)
        parameters = Op.get_parameter(context, request_def, 'parameters', is_required=False, render=2) 
        headers = Op.get_parameter(context, request_def, 'headers', is_required=False, render=2)
        body_format = Op.get_parameter(context, request_def, 'body_format', is_required=False, render=2)
        body = Op.get_parameter(context, request_def, 'body', is_required=False, render=2)
        if body is not None and body_format is None:
            raise UserError("body_format is required when request body is provided (e.g. \"json\", \"form_urlencoded\", etc)")
        
        # Extract response def
        # todo: do we have any use case when we need to render=2. In this case ninja can be used to dinamically set targer_table_addr -> danger: DataLoader will open too many connections!
        response_def = self.op_def.get('response', {})
        target_source_name = Op.get_parameter(context, response_def, 'source', is_required=False, render=3)
        target_database_name = Op.get_parameter(context, response_def, 'database', is_required=False, render=3)
        target_namespace_name = Op.get_parameter(context, response_def, 'namespace', is_required=False, render=3)
        target_table_name = Op.get_parameter(context, response_def, 'table', is_required=False, render=3)
        target_table_addr = TableAddress(target_database_name, target_namespace_name, target_table_name)
        # Compile parser response function code
        parser = response_def.get('parser')
        # todo: do we have any use case to allow non-expression parser?
        if parser:
            raise UserError("parser is not supported. Use parser_expression instead")
        parse_response_expression = response_def.get('parser_expression')
        parse_response_expression_line = Common.get_line_number(response_def, 'parser_expression')
        parse_response_fun_compiled = load_user_function(parse_response_expression, "parser_expression", parse_response_expression_line)
        parse_response_fun = UserFunction(parse_response_fun_compiled, parse_response_expression_line)

        auth_handler = None
        oauth_session = None
        if http_source_name:
            http_source_auth_def = Source.get_parameter(context, http_source_def, 'auth')
            http_source_auth_type = Source.get_parameter(context, http_source_auth_def, 'type', is_required=True)
            if http_source_auth_type == 'basic_auth':
                http_source_auth_username = Source.get_parameter(context, http_source_auth_def, 'username')
                http_source_auth_password = Source.get_parameter(context, http_source_auth_def, 'password')
                auth_handler = HTTPBasicAuth(http_source_auth_username, http_source_auth_password)
            elif http_source_auth_type == 'bearer_token':
                http_source_auth_token = Source.get_parameter(context, http_source_auth_def, 'token')
                auth_handler = BearerTokenAuth(http_source_auth_token)
            elif http_source_auth_type == 'digest_auth':
                http_source_auth_username = Source.get_parameter(context, http_source_auth_def, 'username')
                http_source_auth_password = Source.get_parameter(context, http_source_auth_def, 'password')
                auth_handler = HTTPDigestAuth(http_source_auth_username, http_source_auth_password)
            elif http_source_auth_type == 'api_key':
                http_source_auth_key_name = Source.get_parameter(context, http_source_auth_def, 'key_name')
                http_source_auth_key_value = Source.get_parameter(context, http_source_auth_def, 'key_value')
                http_source_auth_add_to = Source.get_parameter(context, http_source_auth_def, 'add_to')
                auth_handler = APIKeyAuth(http_source_auth_key_name, http_source_auth_key_value, http_source_auth_add_to)
            elif http_source_auth_type == 'oauth1':
                raise UserError("oauth1 auth is not supported yet")
            elif http_source_auth_type == 'oauth2':
                http_source_auth_grant_type = Source.get_parameter(context, http_source_auth_def, 'grant_type')
                if http_source_auth_grant_type == 'password': # 'client_credentials':
                    http_source_auth_token_endpoint = Source.get_parameter(context, http_source_auth_def, 'token_endpoint')
                    http_source_auth_client_id = Source.get_parameter(context, http_source_auth_def, 'client_id')
                    http_source_auth_client_secret = Source.get_parameter(context, http_source_auth_def, 'client_secret')
                    http_source_auth_username = Source.get_parameter(context, http_source_auth_def, 'username')
                    http_source_auth_password = Source.get_parameter(context, http_source_auth_def, 'password')
                    authlib_session = OAuth2Session(http_source_auth_client_id, http_source_auth_client_secret)
                    oauth_session = OAuth2PasswordFlowSession(authlib_session, http_source_auth_token_endpoint, http_source_auth_client_id, http_source_auth_client_secret, http_source_auth_username, http_source_auth_password)
            else:
                raise UserError(f"Unsupported auth type: {http_source_auth_type}")

        http_req_params = HTTPRequestParameters(auth_handler, oauth_session, url, method, parameters, headers, body_format, body, parse_response_fun)


        if op_options.get("debug_foreach_record") or op_options.get("debug_request_preview_trace") or op_options.get("debug_request_preview_pretty"):
            foreach_row_json = op_options.get("debug_foreach_record")
            if foreach_row_json is not None:
                try:
                    foreach_row_dict = json.loads(foreach_row_json)
                except json.JSONDecodeError as e:
                    raise UserError(f"Cannot parse --debug_foreach_record as JSON:" + str(e))
                logger.info("Running in debug_foreach_record mode")
                foreach_row = Row.from_dict(foreach_row_dict)
                context.set_variable(foreach_var_name, foreach_row)
            if op_options.get("debug_request_preview_trace") or op_options.get("debug_request_preview_pretty"):
                logger.info("Running in debug_request_preview_trace mode")
                self._make_request_helper(context, http_req_params, op_options, logger)
            else:
                self._make_request(context, http_req_params, op_options, logger)
        else:
            self.data_loader = DataLoader(self.proj, target_source_name, target_table_addr)
            try:
                if foreach_def is None:
                    self._make_request(context, http_req_params, op_options, logger)
                else:
                    foreach_source_name, foreach_table_addr = parse_foreach_def()
                    foreach_source = self.proj.get_source(foreach_source_name)
                    with foreach_source.connect() as conn:
                        conn.open_table_for_read(foreach_table_addr)
                        foreach_row_count = 0
                        foreach_row = conn.next_row()
                        while foreach_row is not None:
                            foreach_row_count += 1
                            context.set_variable(foreach_var_name, foreach_row)
                            self._make_request(context, http_req_params, op_options, logger)
                            foreach_row = conn.next_row()
            finally:
                self.data_loader.close()

        logger.info(f"Finished \"" + self.get_title() + "\"")
