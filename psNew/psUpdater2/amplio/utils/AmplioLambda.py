import binascii
import copy
import inspect
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from json import JSONEncoder
from typing import Annotated, Any, Dict, Callable, Tuple, Union, get_type_hints

import amplio.rolemanager.manager as roles_manager

roles_manager.open_tables()

LambdaDict = Dict[str, Any]


class LambdaEvent(dict):
    pass


class LambdaContext(dict):
    pass


from enum import Enum, auto
class _ParamSource(Enum):
    """
    An Enum to describe the various places in the AWS Lambda function "event" parameter in which the actual args
    can be found, or an function to provide the value.
    """
    def _check_callable(x):
        """
        A checker for a "PROVIDER_FUNCTION" argument, ...foo: ProviderFunction[<type>, my_provider_function]... The
        provided "my_provider_function" should take the Lambda function "event" and "context" as the first two
        arguments. If additional values are passed to ProviderFunction[...], they are passed as a *args value
        to the provider function.
        :raise: SyntaxError if insufficient values are passed to ProviderFunction[...], or if the second value
            is not a function as determined by inspect.isfunction().
        :return: None
        """
        if not isinstance(x, tuple) or len(x)<2 or not inspect.isfunction(x[1]):
            raise SyntaxError('Requires function(event,context,...) as second argument')

    def _check_index(x):
        """
        A checker for a "PATH_PARAM" argument, ...foo: PathParam[<type>, int]... The
        provided int is used to select the slash-delimited value from the path parameter.
        :raise: SyntaxError if insufficient values are passed to PathParam[...], or if the second value
            is not an int.
        :return: None
        """
        if not isinstance(x, tuple) or len(x)<2 or not isinstance(x[1], int):
            raise SyntaxError('Requires int as second argument')

    CLAIM = object()
    QUERY_STRING = object()
    PATH_PARAM = (object(), _check_index)
    PROVIDER_FUNCTION = (object(), _check_callable)
    BIN_BODY = object()
    JSON_BODY = object()

    def __repr__(self):
        return '<%s.%s>' % (self.__class__.__name__, self.name)

    def check(self, type_hint):
        """
        If this ParamSource requires additional values, check that they're present and of a correct type. See
        "_check_callable" and "_check_index", above.
        :param type_hint: The value(s) passed to the ParamSource, like PathParam[int, 2].
        """
        if isinstance(self.value, tuple) and len(self.value) >= 2:
            self.value[1](type_hint)

class AnnotationFactory:
    def __init__(self, arg_source):
        self.arg_source = arg_source

    def __getitem__(self, type_hint):
        self.arg_source.check(type_hint)
        if isinstance(type_hint, tuple):
            return Annotated[type_hint[0], (self.arg_source,) + type_hint[1:]]
        else:
            return Annotated[type_hint, self.arg_source]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.arg_source})"

    def __eq__(self, other):
        try:
            return self.arg_source == other.arg_source
        except:
            return False

QueryStringParam = AnnotationFactory(_ParamSource.QUERY_STRING)
Claim = AnnotationFactory(_ParamSource.CLAIM)
PathParam = AnnotationFactory(_ParamSource.PATH_PARAM)
ProviderFunction = AnnotationFactory(_ParamSource.PROVIDER_FUNCTION)
BinBody = AnnotationFactory(_ParamSource.BIN_BODY)
JsonBody = AnnotationFactory(_ParamSource.JSON_BODY)

def path_param(n: int):
    def _get_path_param_n(event: LambdaEvent, context: LambdaContext) -> str:
        params = event.get('pathParameters', {}).get('proxy', '').split('/')
        return params[n] if n < len(params) else None

    return _get_path_param_n


def camel_to_snake(string: str) -> str:
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()


def snake_to_camel(string: str) -> str:
    words = string.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


# subclass JSONEncoder so that we can encode date & datetime
class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()


def response(status_code: int, body: Dict) -> Dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": '*',
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, cls=DateTimeEncoder)
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


def _prepare_response_content(response: Any, response_model) -> Any:
    if isinstance(response, list):
        return [
            _prepare_response_content(item, response_model)
            for item in response
        ]
    return response


def _gather_args(handler: Callable, event: LambdaEvent, context: LambdaContext) -> Dict[str, Any]:
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
        print(f'Looking for {key} in {values}')
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
            body_bytes = binascii.a2b_base64(body)  # if event.get('isBase64Encoded', False) else bytes(body)
        return body_bytes
             
    def coerce_param(param, param_type):
        def bool_param():
            """:return: the truthiness of param as True or False, or None if no determination can be made."""
            try:
                val = str(param).lower()
                if val in ('y', 'yes', 't', 'true', 'on', '1'): return True
                elif val in ('n', 'no', 'f', 'false', 'off', '0'): return False
            except Exception: pass
            return None
        def int_param():
            try: return int(param)
            except Exception: pass
        if param_type is None or param is None: return param
        if param_type==bool: param = bool_param()
        if param_type==int: param = int_param()
        return param
            
    # What arguments does the handler want?
    handler_params = inspect.signature(handler).parameters
    type_hints = get_type_hints(handler)

    # Parse query params
    query_args = event.get('queryStringParameters', {})
    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})

    # Extract params from event and context.
    annotated_type = type(Annotated[object, None])
    kwargs = {}
    for param_name, param in handler_params.items():
        # print(f'param: {param_name}, annotation: {param.annotation}, type(annotation): {type(param.annotation)}')
        param_source: _ParamSource = _ParamSource.QUERY_STRING
        param_type: type = type_hints[param_name]
        if type(param.annotation) is annotated_type:
            annotation_md = param.annotation.__metadata__[0]
            if isinstance(annotation_md, tuple):
                md_args = list(annotation_md)[1:]
                param_source = annotation_md[0]
            else:
                md_args = []
                param_source = annotation_md
        elif param.annotation in [JsonBody, BinBody, Claim, PathParam, QueryStringParam, ProviderFunction]:
            md_args = []
            param_source = param.annotation.arg_source

        print(f'Param {param_name}, source {param_source}')
        # NOTE: At one time there were numerous getters here. Most were unused,
        # and have been removed. Add new ones only when actually needed.
        # “Entities should not be multiplied beyond necessity.”

        # It is not possible to pass "None" in the lambda event, so we can use this
        # to mean "no value provided".
        param_value = None

        # Is the parameter the entire body as JSON?
        if param_source == _ParamSource.JSON_BODY:
            param_value = get_body_json()

        # Is the parameter the entire body as a base-64 encoded string?
        elif param_source == _ParamSource.BIN_BODY:
            param_value = get_body_bytes()

        # Is the parameter from the querystring?
        elif param_source == _ParamSource.QUERY_STRING:
            param_value = get_snakish(query_args, param_name)

        # Is the parameter one of the claims?
        elif param_source == _ParamSource.CLAIM:
            param_value = get_snakish(claims, param_name)

        elif param_source == _ParamSource.PATH_PARAM:
            path_ix = md_args[0] # which path component?
            path_params = event.get('pathParameters', {}).get('proxy', '').split('/')
            param_value =  path_params[path_ix] if path_ix < len(path_params) else None

        elif param_source == _ParamSource.PROVIDER_FUNCTION:
            fn = md_args[0] # get provider function
            if inspect.isgeneratorfunction(fn):
                param_value = next(fn())
            else:
                fn_args = md_args[1:]
                param_value = fn(event, context, *fn_args)

        # Is the parameter 'event' or 'context'?
        elif (param.annotation == type(LambdaEvent()) or param_name == 'event'):
            param_value = event
        elif (param.annotation == type(LambdaContext) or param_name == 'context'):
            param_value = context

        if param_value is not None:
            param_value = coerce_param(param_value, param_type)
            kwargs[param_name] = param_value

    return kwargs


@dataclass
class Handler():
    function: Callable[..., Tuple[Any, int]]
    allowed_roles: str = 'AD,PM,CO,FO'
    get_program_id: Callable[..., str] = None


def handler(_func=None, *, roles: str = 'AD,PM,CO,FO', get_programid: Callable[..., str] = None, action=None) -> Any:
    """
    Helper to make it easier to write Lambda "handler" functions. These may be
    called by the Lambda runtime, or may be dispatched to from a "router"
    function.

    The returned value of the decorated Callable will be JSON encoded and
    returned as the 'body' of the response, with this important caveat: If the
    Callable returns a tuple of length 2, and the second element in the tuble is
    an int, the first element will be returned as the 'body', and the second
    element will be returned as the HTTP status code.

    To return a tuple of length 2 with an int as the second element, construct a
    tuple with the desired values, and return that as the first element, and
    None as the second:
        x = tuple({'a':'a'}, 123)
        return x, None
    Alternatively, specify the HTTP status code as the second returned element:
        return x, 200

    Parse request and response paramenters.

    params:
    roles: str  Optional.  If provided, requires the user (as passed in claims)
        has one of the requested roles in the given program. If not provided,
        'AD,PM,CO,FO' is assumed, that is, any role in the program.
    programid: Callable. Optional. Used only if 'roles' are required.
        If the handler takes 'programid' as an argument, and most will, that is
        used for the program. Any handler that doesn't take such a parameter
        must provide a callable to extract the programid from the event, the
        environment, or somewhere. NOTE: program_id, programId, and project are
        recognized as aliases for programid.

    Example:
          @handler(roles='AD,PM')
          def get_content(programid: QueryStringParam):
            . . .
      See full example below.
    """
    global LAMBDA_HANDLERS

    def decorator(_func: Callable[..., Tuple[Any, int]]) -> Any:
        nonlocal action
        action = action or _func.__name__
        LAMBDA_HANDLERS[action] = Handler(_func, allowed_roles=roles, get_program_id=get_programid)
        # _func.needed_roles = roles
        return _func

    try:
        if LAMBDA_HANDLERS is None:
            LAMBDA_HANDLERS = {}
    except Exception as ex:
        LAMBDA_HANDLERS = {}

    if _func is None:
        return decorator
    else:
        return decorator(_func)


class LambdaRouter():
    """
    Class implementing a Lambda Handler helper. When provided with the event and
    context from a Lambda event, and with the action to be performed (presumably
    extracted from the event), the object can dispatch the desired function with
    the proper arguments.

    Sample usage:
        @handler(role='AD,PM') # user must be configured with AD or PM role in the program.
        def some_pm_function(programid: queryStringParam, data_desired: JsonBody):
            result, http_response_code = do_pm_work(programid, data_desired)
            return result, http_response_code

        @handler # any role will do -- implied ('AD,PM,CO,FO')
        def some_useful_function(programid: QueryStringParam, data_desired: QueryStringParam):
            result = do_real_work(programid, data_desired)
            return result # implied 200

        def lambda_handler(event, context):
            the_router = LambdaRouter(event, context)
            action = the_router.pathParam(0) # or however else the action is determined.
            the_router.dispatch(action)
    """

    def __init__(self, event, context, handlers: Dict[str, Union[Callable, Handler]] = None):
        global LAMBDA_HANDLERS
        self._event = event
        self._context = context
        if handlers is None:
            handlers = LAMBDA_HANDLERS
        self._handlers = {n: h if isinstance(h, Handler) else Handler(h) for n, h in handlers.items()}

    @property
    def queryStringParams(self):
        return self._event.get('queryStringParameters', {})

    def queryStringParam(self, name: str):
        return self._event.get('queryStringParameters', {}).get(name)

    @property
    def path_params(self):
        return self._event.get('pathParameters', {}).get('proxy', '').split('/')

    def path_param(self, index: int):
        params = self._event.get('pathParameters', {}).get('proxy', '').split('/')
        return params[index] if index < len(params) else None

    def claim(self, name: str):
        return self._event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get(name)

    def _determine_programid(self, handler: Handler, params) -> str:
        if handler.get_program_id is not None:
            return handler.get_program_id(self._event, self._context)
        for x in [n for n in ['programid', 'program_id', 'programId', 'project'] if n in params]:
            return params[x]
        for x in [n for n in ['programid', 'program_id', 'programId', 'project'] if n in self.queryStringParams]:
            return self.queryStringParams[x]

    def _user_has_allowed_role(self, allowed_roles, email, programid) -> bool:
        return roles_manager.user_has_role_in_program(email=email, program=programid, roles=allowed_roles)

    def dispatch(self, action: str) -> Any:
        if action not in self._handlers:
            raise Exception(f'Error: Lambda router has no action {action}')
        handler = self._handlers[action]
        kwargs = _gather_args(handler.function, self._event, self._context)
        try:
            allowed_roles = handler.allowed_roles
        except Exception as ex:
            allowed_roles = None
        if allowed_roles:
            programid = self._determine_programid(handler, kwargs)
            if not self._user_has_allowed_role(allowed_roles, self.claim('email'), programid):
                return response(status_code=403,
                                body={'message': 'This user does not have an appropriate role in the program'})

        result: Any = None
        returned_value = handler.function(**kwargs)
        if isinstance(returned_value, tuple) and len(returned_value) == 2 and (
                isinstance(returned_value[1], int) or returned_value[1] is None):
            result = returned_value[0]
            status_code = returned_value[1]
        else:
            result = returned_value
            status_code = None
        # Response with body and http status code.
        if result is None:
            return response(status_code or 404, {"error": "Not found"})
        elif isinstance(result, Exception):
            return exception_response(result)

        return response(status_code=status_code or 200, body=result)
