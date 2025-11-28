from rest_framework.response import Response # type: ignore
from django.http import JsonResponse # type: ignore
from .global_service import DatabaseModel
from .models import ignore_calls,capability,user
from plmp_backend.env import SIMPLE_JWT
import jwt # type: ignore
from rest_framework import status # type: ignore
from rest_framework.renderers import JSONRenderer # type: ignore
import traceback


def check_ignore_authentication_for_url(request):
    path = request.path.split("/")
    # try:
    #     action = path[2] 
    # except IndexError:
    #     return False 
    result_obj = DatabaseModel.get_document(ignore_calls.objects, {"name__in": path})
    return result_obj is not None  
def ensure_serializable(data, path="root"):
    """
    Recursively ensure all data is JSON serializable.
    Converts model instances to their ID or string representation.
    """
    if data is None:
        return None
    
    if isinstance(data, (str, int, float, bool)):
        return data
    
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            result[key] = ensure_serializable(value, path=f"{path}.{key}")
        return result
    
    if isinstance(data, (list, tuple)):
        return [ensure_serializable(item, path=f"{path}[{i}]") for i, item in enumerate(data)]
    
    # Handle Django model instances
    if hasattr(data, 'pk'):
        print(f"⚠️ Found model instance at {path}: {data.__class__.__name__} (pk={data.pk})")
        return data.pk
    
    # Handle QuerySet
    if hasattr(data, '__iter__') and hasattr(data, 'model'):
        print(f"⚠️ Found QuerySet at {path}: {data.model.__name__}")
        return list(data.values())
    
    # Handle any other non-serializable object
    print(f"⚠️ Found non-serializable object at {path}: {data.__class__.__name__}")
    
    # Try to convert to dict if possible
    if hasattr(data, '__dict__'):
        return ensure_serializable(data.__dict__, path=f"{path}.__dict__")
    
    # Fallback to string representation
    try:
        return str(data)
    except Exception:
        return f"<Non-serializable: {type(data).__name__}>"
def skip_for_paths():
    """
    Decorator for skipping middleware based on path
    """
    def decorator(f):       
        def check_if_health(self, request):
            if check_ignore_authentication_for_url(request): 
                return self.get_response(request)  
            return f(self, request) 
        return check_if_health
    return decorator

def createJsonResponse1(message='success', status=True, data=None):
    """Create a JSON response with a message, status, and additional data."""
    response_data = {
        'data': data,
            'message': message,
            'status': status
    }
    return JsonResponse(response_data, content_type='application/json', status=200)
def createJsonResponse(request, token = None):
    c1 = ''
    if token:
        header,payload1,signature = str(token).split(".")
        c1 = header+'.'+payload1
    else:
        data_map = dict()
        data_map['data'] = dict()
        response = Response(content_type = 'application/json') 
        response.data = data_map
        response.data['emessage'] = 'success'
        response.data['estatus'] = True
        # response.data['_c1'] = c1
        response.status_code = 200
        return response
        c1=request.COOKIES.get('_c1')
    data_map = dict()
    data_map['data'] = dict()
    response = Response(content_type = 'application/json') 
    response.data = data_map
    response.data['emessage'] = 'success'
    response.data['estatus'] = True
    response.data['_c1'] = c1
    response.status_code = 200
    return response


def check_authentication(request):
    token=""
    c1=request.COOKIES.get('_c1')
    c2=request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1+"."+c2
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT
    except Exception as e:
        return validationObjJWT
    return validationObjJWT


def refresh_cookies(request,response):
    token=""
    c1=request.COOKIES.get('_c1')
    c2=request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1+"."+c2
    createCookies(token, response)


def obtainUserObjFromToken(request):
    token = ""
    c1 = request.COOKIES.get('_c1')
    c2 = request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1 + "." + c2
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        # return validationObjJWT["id"],validationObjJWT["name"],validationObjJWT["email"]
        return validationObjJWT
    except Exception as e:
        return validationObjJWT


def check_role_and_capability(request, role_name):
    path = request.path.split("/")
    action = path[2] if len(path) >= 3 else None
    is_accessible = False
    capability_obj = DatabaseModel.get_document(capability.objects, {"action_name": action, "role_list__in": [role_name]})
    
    
    if capability_obj != None:
        is_accessible = True 
    else:
        print('❌ NO ACCESS - Role does not have permission for this action')
    
    return is_accessible
import threading

# Thread-local storage to store user_login_id
_thread_locals = threading.local()

def get_current_user():
    return getattr(_thread_locals, 'user_login_id', None)
def get_current_client():
    return getattr(_thread_locals, 'client_id', None)
class CustomMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
    @skip_for_paths()
    def __call__(self, request):
        response = createJsonResponse(request)
        try:
            user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
            _thread_locals.user_login_id = user_login_id
            user_login_obj = DatabaseModel.get_document(user.objects,{'id':user_login_id})
            if user_login_obj != None:
                role = user_login_obj.role
                if user_login_obj.role == 'super-admin':
                    # _thread_locals.login_client_id = str("super-admin")
                    client_id = ""
                else:
                    # _thread_locals.login_client_id = str(user_data_obj.client_id.id)
                    client_id = str(user_login_obj.client_id.id)
                _thread_locals.client_id = client_id
                
                # refresh_cookies(request, response)
                if check_role_and_capability(request, role):
                    res = self.get_response(request)
                    if isinstance(res, Response):
                        print("=" * 60)
                        print("📦 RAW RESPONSE DATA FROM VIEW:")
                        print(f"Type: {type(res.data)}")
                        try:
                            import json
                            print(json.dumps(res.data, indent=2, default=lambda o: f"<{o.__class__.__name__}: {getattr(o, 'pk', 'N/A')}>"))
                        except:
                            print(res.data)
                        print("=" * 60)
                        response.data['data'] = ensure_serializable(res.data)
                        if isinstance(res.data, dict):
                            if res.data.get('STATUS_CODE') == 401:
                                response.status_code = status.HTTP_401_UNAUTHORIZED
                    else:
                        response.data['data'] = res
                        if isinstance(res, dict):
                            if res.get('STATUS_CODE') == 401:
                                response.status_code = status.HTTP_401_UNAUTHORIZED
                else:
                    response.status_code = status.HTTP_401_UNAUTHORIZED
            else:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data['message'] = 'Invalid token'
        except Exception as e:
            print("=" * 60)
            print("🔴 ACTUAL ERROR:")
            print("=" * 60)
            print(f"Exception: {e.__class__.__name__}: {str(e)}")
            traceback.print_exc()
            print("=" * 60)
            if e.__class__.__name__ in ['ExpiredSignatureError', 'DecodeError']:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data['message'] = 'Invalid token'
            elif e.__class__.__name__ == 'ValidationError':
                response.status_code = status.HTTP_400_BAD_REQUEST
                response.data['message'] = str(e)
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                response.data['message'] = f'Error: {str(e)}'
        response.accepted_renderer = JSONRenderer()
        response.accepted_media_type = "application/json"
        response.renderer_context = {}
        response.data = ensure_serializable(response.data)
        try:
            response.render()
        except TypeError as e:
            print("=" * 60)
            print("🔴 SERIALIZATION ERROR:")
            traceback.print_exc()
            print("=" * 60)
        
        # Return safe error response
            error_response = Response({
            'data': None,
            'emessage': f'Serialization error: {str(e)}',
            'estatus': False
            })
            error_response.status_code = 500
            error_response.accepted_renderer = JSONRenderer()
            error_response.accepted_media_type = "application/json"
            error_response.renderer_context = {}
            error_response.render()
            return error_response
        
        return response


def createCookies(token,response):
    header,payload,signature = str(token).split(".")
    response.set_cookie(
        key = "_c1",
        value = header+"."+payload,
        max_age = SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        secure = SIMPLE_JWT['AUTH_COOKIE_SECURE'],
        httponly = False,
        samesite = SIMPLE_JWT['AUTH_COOKIE_SAMESITE'],
        domain = SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )
    response.set_cookie(
        key = "_c2",
        value = signature,
        expires = SIMPLE_JWT['ACCESS_TOKEN_LIFETIME'],
        secure = SIMPLE_JWT['AUTH_COOKIE_SECURE'],
        httponly = True,
        samesite = SIMPLE_JWT['AUTH_COOKIE_SAMESITE'],
        domain = SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )