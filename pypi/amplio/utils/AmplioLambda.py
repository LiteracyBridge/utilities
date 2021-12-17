import binascii
import functools
import inspect
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Callable, Tuple, Union

import amplio.rolemanager.manager as roles_manager
from pydantic.main import ModelMetaclass

roles_manager.open_tables()

LambdaDict = Dict[str, Any]

class LambdaEvent(dict):
    pass


class BodyParam():
    pass


class JsonBody(dict):
    pass


class BinBody(bytes):
    pass


class QueryStringParams(dict):
    pass


class QueryStringParam(str):
    pass


class Claim(str):
    pass


def path_param(n: int):
    def _get_path_param_n(event: LambdaEvent, context: LambdaContext) -> str:
        params = event.get('pathParameters', {}).get('proxy', '').split('/')
        return params[n] if n < len(params) else None

    return _get_path_param_n


class LambdaCognitoIdentity(object):
    cognito_identity_id: str
    cognito_identity_pool_id: str


class LambdaClientContextMobileClient(object):
    installation_id: str
    app_title: str
    app_version_name: str
    app_version_code: str
    app_package_name: str


class LambdaClientContext(object):
    client: LambdaClientContextMobileClient
    custom: LambdaDict
    env: LambdaDict


class LambdaContext(object):
    function_name: str
    function_version: str
    invoked_function_arn: str
    memory_limit_in_mb: int
    aws_request_id: str
    log_group_name: str
    log_stream_name: str
    identity: LambdaCognitoIdentity
    client_context: LambdaClientContext

    @staticmethod
    def get_remaining_time_in_millis() -> int:
        return 0


def camel_to_snake(string: str) -> str:
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()


def snake_to_camel(string: str) -> str:
    words = string.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


def response(status_code: int, body: Dict) -> Dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": '*',
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body)
    }


# noinspection PyUnresolvedReferences
def exception_response(ex: Exception, status_code: int = 500, message: str = None) -> Dict:
    message = message or str(ex)
    try:
        status_code = ex.status_code
    except:
        pass
    try:
        message = ex.message
    except:
        pass
    return response(status_code, {"error": message})


def get_typed_signature(call: Callable[..., Any]) -> inspect.Signature:
    """
    Given a callable, return a description of the signature.
    """
    signature = inspect.signature(call)
    typed_params = [
        inspect.Parameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            annotation=param.annotation,
        )
        for param in signature.parameters.values()
    ]
    typed_signature = inspect.Signature(typed_params)
    return typed_signature


def _prepare_response_content(response: Any, response_model) -> Any:
    if isinstance(response, list):
        return [
            _prepare_response_content(item, response_model)
            for item in response
        ]
    return response


def _gather_params(handler: Callable, event: LambdaEvent, context: LambdaContext) -> Dict[str, Any]:
    """
    Given a function to be called in an AWS Lambda context, and the event and context objects passed 
    to a lambda handler, use the function signature to extract the parameters that the function needs.

    params:
      handler: a callable, the function to be supplied with parameters from the Lambda call.
      event: the event object passed to the lambda handler.
      context: the context object passed to the lambda handler.

    return:
      A dictionary of {str: Any}. The keys are the names of the parameters.
    """

    def get_snakish(values: Dict[str, Any], key: str) -> Any:
        """
        Look up a parameter's value in the Dict. If the parameter is not found, try munging
        the name from sake_case to camelCase, and if such a value is found, return that.
        """
        if key in values:
            return values[key]
        key = snake_to_camel(key)
        if key in values:
            return values[key]
        return None

    body_json = None
    body_bytes = None

    def get_body_json():
        """
        Return the "body" member of the event object, interpreted as a json object.
        """
        nonlocal body_json
        if body_json is None:
            body_json = json.loads(event["body"])
        return body_json

    def get_body_bytes():
        """
        Return the "body" member of the event object, as bytes. If the bytes are base-64 encoded,
        decode them first.
        """
        nonlocal body_bytes
        if body_bytes is None:
            body = bytes(event["body"], 'utf-8')
            # The event lies about isBase64Encoded. It is.
            body_bytes = binascii.a2b_base64(body) #if event.get('isBase64Encoded', False) else bytes(body)
        return body_bytes

    # What arguments does the handler want?
    endpoint_signature = get_typed_signature(handler)
    handler_params = endpoint_signature.parameters

    # Parse query params
    query_args = {}
    if 'queryStringParameters' in event and event['queryStringParameters']:
        query_args = event['queryStringParameters']
    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})

    # Extract params from event and context.
    params = {}
    for param_name, param in handler_params.items():
        # print(f'param: {param_name}, annotation: {param.annotation}, type(annotation): {type(param.annotation)}')
        # Is the parameter from the body?
        if (param.annotation == type(BodyParam())):
            params[param.name] = get_snakish(get_body_json(), param.name)

        # Is the parameter the entire body as JSON?
        elif (param.annotation == type(JsonBody())):
            params[param.name] = get_body_json()

        # Is the parameter the entire body as a base-64 encoded string?
        elif (param.annotation == type(BinBody())):
            params[param.name] = get_body_bytes()

        # Is the parameter from the querystring?
        elif (param.annotation == type(QueryStringParam())):
            params[param.name] = get_snakish(query_args, param.name)

        # Is the parameter ALL the query string params?
        elif (param.annotation == type(QueryStringParams())):
            params[param.name] = query_args

        # Is the parameter one of the claims?
        elif (param.annotation == type(Claim())):
            params[param.name] = get_snakish(claims, param.name)

        # Is the parameter a generator function? Generate it.
        elif inspect.isgeneratorfunction(param.default):
            params[param.name] = next(param.default())

        # Is the type of the parameter 'function'? We'll call the function to get the value.
        elif inspect.isfunction(param.default):
            params[param.name] = param.default(event, context)

        # Is the type of the parameter a ModelMetaclass, whatever that is?
        elif isinstance(param.annotation, ModelMetaclass):
            params[param.name] = param.annotation.parse_obj(get_body_json())

        # Is the parameter 'event' or 'context'?
        elif (param.annotation == type(LambdaEvent()) or param.name == 'event'):
            params[param.name] = event
        elif (param.annotation == type(LambdaContext) or param.name == 'context'):
            params[param.name] = context

    return params


def router() -> Any:
    """
    Helper to make it easier to write Lambda "router" functions, where a router function is called by
    the Lambda runtime, and dispatches to one of possibly several functions, based on parameters in the
    Lambda event. Typically the parameters will be path parameters or query string parameters, though they
    can be from the body, or computed from the event and/or context by another helper.

    The router function is decorated with "@router" or "@router()". The function will probably take
    one or more paramters for the multiple dispatch, such as "action". The function will probably need to
    take "event" and "context" parameters, to forward on to the handlers.

    The returned value from this router function is returned as-is to the Lambda runtime.
    """

    def decorator(handler: Callable) -> Any:
        @functools.wraps(handler)
        def wrapper(event: LambdaEvent, context: LambdaContext):
            # pathParameters = event.get('pathParameters', {}).get('proxy', '').split('/')
            # return handler(pathParameters, event, context)
            params = _gather_params(handler, event, context)
            return handler(**params)

        return wrapper

    return decorator


def handler(roles: str = None, programid: Callable[..., str] = None) -> Any:
    """
    Helper to make it easier to write Lambda "handler" functions. These may be called by the Lambda
    runtime, or may be dispatched to from a "router" function. 

    The returned value of the decorated Callable will be JSON encoded and returned as the 'body' of the
    response, with this important caveat: If the Callable returns a tuple of length 2, and the second
    element in the tuble is an int, the first element will be returned as the 'body', and the second
    elementn will be returned as the HTTP status code.

    To return a tuple of length 2 with an int as the second element, construct a tuple with the desired
    values, and return that as the first element, and None as the second:
        x = tuple({'a':'a'}, 123)
        return x, None
    Alternatively, specify the HTTP status code as the second returned element:
        return x, 200

    Parse request and response paramenters.

    params:
    roles: str  Optional.  If provided, requires the user (as passed in claims) has one of the requested
        roles in the given program.
    programid: Callable. Optional. Used only if the 'roles' parameter is given.  If the handler takes "programid'
        as an argument, and most will, that is used for the program. Any handler that doesn't take such a parameter
        must provide a callable to extract the programid from the (event, context).
    """

    def decorator(handler: Callable[..., Tuple[Any, int]]) -> Any:
        @functools.wraps(handler)
        def wrapper(event: LambdaEvent, context: LambdaContext):
            params = _gather_params(handler, event, context)
            if roles is not None:
                email = event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get('email')
                progid = params.get('programid') or programid(event, context)
                if not roles_manager.user_has_role_in_program(email=email, program=progid, roles=roles):
                    return response(403, {'message': 'This user does not have an appropriate role in the program'})

            result: Any = None
            returned_value = handler(**params)
            if isinstance(returned_value, tuple) and len(returned_value) == 2 and (
                    isinstance(returned_value[1], int) or returned_value[1] is None):
                result = returned_value[0]
                status_code = returned_value[1]
            else:
                result = returned_value
                status_code = None
            # Response with body and http status code.
            if not result:
                return response(status_code or 404, {"error": "Not found"})
            elif isinstance(result, Exception):
                return exception_response(result)

            return response(status_code or 200, result)

        return wrapper

    return decorator

@dataclass
class Handler():
    function: Callable[..., Tuple[Any, int]]
    get_program_id: Callable[..., str] = None

def NeedsRole(needed_roles: str) -> Callable:
    def decorator_needs_roles(func: Callable) -> Callable:
        func.needed_roles = needed_roles
        return func
    return decorator_needs_roles

class LambdaRouter():
    """
    Class implementing a Lambda Handler helper. When provided with the event and context from a
    Lambda event, and with a dictionary of handler functions, and with the action to be performed
    (presumably extracted from the event), the object can dispatch the desired function with the
    proper arguments.

    Uses the INSTEDD argument decorations to describe how to obtain the function's arguments.

    Sample usage:
        @NeedsRole('AD,PM')
        def some_useful_function(programid: QueryStringParam, data_desired: QueryStringParam):
            result = do_real_work(programid, data_desired)
            return result
        MY_HANDLERS = {'query': some_useful_function}
        def lambda_handler(event, context):
            the_router = LambdaRouter(event, context, handlers=MY_HANDLERS)
            action = the_router.pathParam(0)
            the_router.dispatch(action)
    """
    def __init__(self, event, context, handlers: Dict[str, Union[Callable, Handler]]):
        self._event = event
        self._context = context
        self._handlers = {n:h if isinstance(h, Handler) else Handler(h) for n,h in handlers.items()}

    @property
    def queryStringParams(self):
        return self._event.get('queryStringParameters', {})
    def queryStringParam(self, name: str):
        return self._event.get('queryStringParameters', {}).get(name)
    @property
    def pathParams(self):
        return self._event.get('pathParameters', {}).get('proxy', '').split('/')
    def pathParam(self, index: int):
        params = self._event.get('pathParameters', {}).get('proxy', '').split('/')
        return params[index] if index < len(params) else None
    def claim(self, name: str):
        return self._event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get(name)

    def _determine_programid(self, handler: Handler, params) -> str:
        if handler.get_program_id is not None:
            return handler.get_program_id(self._event, self._context)
        for x in [n for n in ['programid', 'program_id', 'programId'] if n in params]:
            return params[x]
        for x in [n for n in ['programid', 'program_id', 'programId'] if n in self.queryStringParams]:
            return self.queryStringParams[x]

    def _user_has_needed_roles(self, roles, email, programid) -> bool:
        return roles_manager.user_has_role_in_program(email=email, program=programid, roles=roles)

    def dispatch(self, action: str) -> Any:
        if action not in self._handlers:
            raise Exception(f'Error: Lambda router has no action {action}')
        handler = self._handlers[action]
        params = _gather_params(handler.function, self._event, self._context)
        try:
            needed_roles = handler.function.needed_roles
        except Exception as ex:
            needed_roles = None
        if needed_roles:
            programid = self._determine_programid(handler, params)
            if not self._user_has_needed_roles(needed_roles, self.claim('email'), programid):
                return response(status_code=403, body={'message': 'This user does not have an appropriate role in the program'})

        result: Any = None
        returned_value = handler.function(**params)
        if isinstance(returned_value, tuple) and len(returned_value) == 2 and (
                isinstance(returned_value[1], int) or returned_value[1] is None):
            result = returned_value[0]
            status_code = returned_value[1]
        else:
            result = returned_value
            status_code = None
        # Response with body and http status code.
        if not result:
            return response(status_code or 404, {"error": "Not found"})
        elif isinstance(result, Exception):
            return exception_response(result)

        return response(status_code=status_code or 200, body=result)

