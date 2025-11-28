from django.http import JsonResponse # type: ignore
from .models import products
from .models import varient_option
from .models import category
from .models import level_one_category
from .models import level_two_category
from .models import level_three_category
from .models import level_four_category
from .models import level_five_category
from .models import product_category_config
from .models import product_varient_option
from .models import product_varient
from .models import category_log
from .models import product_log
from .models import product_varient_log
from .models import category_varient_option_log
from .models import category_varient
from .models import user
from .models import type_name
from .models import type_value
from .models import dimensions
from .models import xl_mapping
from .models import brand
from .models import client
from .models import brand_category_price
from .models import radial_price_log
from .models import revert_varient_retail_price

from plmp_backend.env import MONGODB_COURSE_DB_NAME
from django.http import HttpResponse # type: ignore
from openpyxl import Workbook # type: ignore
import pandas as pd # type: ignore # type: ignoreC
from io import BytesIO
from rest_framework.parsers import JSONParser # type: ignore
import json
from .global_service import DatabaseModel
from bson import ObjectId # type: ignore
from django.http import HttpResponse # type: ignore
from openpyxl import Workbook # type: ignore
from io import BytesIO
import pandas as pd # type: ignore
import pdfplumber # type: ignore
import math
import shutil
from django.core.files.storage import FileSystemStorage # type: ignore
import os
import re
from .view_utils import getCategoryLevelOrder
from django.views.decorators.csrf import csrf_exempt # type: ignore
from django.core.management.utils import get_random_secret_key # type: ignore
from pytz import timezone # type: ignore
from django.utils.timezone import is_naive, make_aware # type: ignore
from .custom_middleware import get_current_user,get_current_client
from mongoengine.connection import get_db
from django.core.cache import cache
from django.http import JsonResponse
import hashlib
import json
def v1(request):
    u_o = DatabaseModel.get_document(user.objects).name
    db = get_db()  # Fetch the active MongoDB connection
    db_name = db.name  # Get the database name
    print(db_name)
    return JsonResponse({"PLMP_API":u_o},safe=False)
def create_user(request):
    json_request = json.loads(request.body)
    categories_data = json_request.get('categories')

    for category_data in categories_data:
        name = category_data.get('name')
        if name:
            category_obj = category(name=name,section_list = [])
            category_obj.save()
    return JsonResponse({'status': 'User created'})


@csrf_exempt
def createCategory(request):
    client_id = get_current_client()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    category_obj = DatabaseModel.get_document(category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if category_obj :
        data['is_created'] = False
        data['error'] = "Category Already Exists In Our Database"
        return data
    else:
        category_obj = DatabaseModel.save_documents(category,{'name':name})
    logForCategory(category_obj.id,"Created",user_login_id,'level-1',{})
    data['is_created'] = True
    return data


@csrf_exempt
def createCategory1(request):
    client_id = get_current_client()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    category_id = json_req.get("category_id")
    level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if level_one_category_obj :
        category_obj = DatabaseModel.get_document(category.objects,{'level_one_category_list__in':level_one_category_obj.id})
        if category_obj:
            data['is_created'] = False
            data['error'] = "Category Already Exists In Our Database"
            return data
    else:
        level_one_category_obj = DatabaseModel.save_documents(level_one_category,{'name':name})
    DatabaseModel.update_documents(category.objects,{"id":category_id},{'add_to_set__level_one_category_list':level_one_category_obj.id})
    logForCategory(level_one_category_obj.id,"Created",user_login_id,'level-2',{})
    data['is_created'] = True
    return data


@csrf_exempt
def createCategory2(request):
    client_id = get_current_client()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    category_id = json_req.get("category_id")
    level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if level_two_category_obj :
        category_obj = DatabaseModel.get_document(category.objects,{'level_two_category_list__in':level_two_category_obj.id})
        if category_obj:
            data['is_created'] = False
            data['error'] = "Category Already Exists In Our Database"
            return data
    else:
        level_two_category_obj = DatabaseModel.save_documents(level_two_category,{'name':name})
    DatabaseModel.update_documents(level_one_category.objects,{"id":category_id},{'add_to_set__level_two_category_list':level_two_category_obj.id})
    logForCategory(level_two_category_obj.id,"Created",user_login_id,'level-3',{})
    data['is_created'] = True
    return data


@csrf_exempt
def createCategory3(request):
    client_id = get_current_client()
    
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    section_id = json_req.get("category_id")
    level_three_category_obj = DatabaseModel.get_document(level_three_category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if level_three_category_obj :
        category_obj = DatabaseModel.get_document(category.objects,{'level_three_category_list__in':level_three_category_obj.id})
        if category_obj:
            data['is_created'] = False
            data['error'] = "Category Already Exists In Our Database"
            return data
    else:
        level_three_category_obj = DatabaseModel.save_documents(level_three_category,{'name':name})
    DatabaseModel.update_documents(level_two_category.objects,{"id":section_id},{'add_to_set__level_three_category_list':level_three_category_obj.id})
    logForCategory(level_three_category_obj.id,"Created",user_login_id,'level-4',{})
    data['is_created'] = True
    return data


@csrf_exempt
def createCategory4(request):
    client_id = get_current_client()
    
    json_req = JSONParser().parse(request)
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    name = json_req.get("name").title()
    section_id = json_req.get("category_id")
    level_four_category_obj = DatabaseModel.get_document(level_four_category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if level_four_category_obj :
        category_obj = DatabaseModel.get_document(category.objects,{'level_four_category_list__in':level_four_category_obj.id})
        if category_obj:
            data['is_created'] = False
            data['error'] = "Category Already Exists In Our Database"
            return data
    else:
        level_four_category_obj = DatabaseModel.save_documents(level_four_category,{'name':name})
    DatabaseModel.update_documents(level_three_category.objects,{"id":section_id},{'add_to_set__level_four_category_list':level_four_category_obj.id})
    logForCategory(level_four_category_obj.id,"Created",user_login_id,'level-5',{})
    data['is_created'] = True
    return data


@csrf_exempt
def createCategory5(request):
    client_id = get_current_client()
    
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    section_id = json_req.get("category_id")
    level_five_category_obj = DatabaseModel.get_document(level_five_category.objects,{'name':name,'client_id':ObjectId(client_id)})
    data = dict()
    if level_five_category_obj :
        category_obj = DatabaseModel.get_document(category.objects,{'level_five_category_list__in':level_five_category_obj.id})
        if category_obj:
            data['is_created'] = False
            data['error'] = "Category Already Exists In Our Database"
            return data
    else:
        level_five_category_obj = DatabaseModel.save_documents(level_five_category,{'name':name})
    DatabaseModel.update_documents(level_four_category.objects,{"id":section_id},{'add_to_set__level_five_category_list':level_five_category_obj.id})
    logForCategory(level_five_category_obj.id,"Created",user_login_id,'level-6',{})
    data = dict()
    data['is_created'] = True
    return data


@csrf_exempt
def createProduct(request):
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    client_id = get_current_client()
    product_obj = json_req.get("product_obj")
    category_id = product_obj["category_id"]
    category_name = product_obj["category_name"]
    products_obj = DatabaseModel.get_document(products.objects,{'product_name':product_obj['product_name'],'client_id':client_id})
    if products_obj:
        data = dict()
        data['status'] = False
        data['error'] = "Product Already Created"
        return data
    # height = ""
    # width = ""
    # depth = ""
    # length = ""
    # if product_obj['height']:
    #     if product_obj['units'] == "in":
    #         height = str(product_obj['height']) +'"'+'H'
    #     elif product_obj['units'] == "ft":
    #         height = str(product_obj['height']) +"'"+'H'
    #     elif product_obj['units'] == "mm":
    #         height = str(product_obj['height']) +"mm"+'H'
    # if product_obj['width']:
    #     if product_obj['units'] == "in":
    #         width = str( product_obj['width']) +'"'+'W'
    #     elif product_obj['units'] == "ft":
    #         width = str( product_obj['width']) +"'"+'W'
    #     elif product_obj['units'] == "mm":
    #         width = str( product_obj['width']) +"mm"+'W'
    # if product_obj['depth']:
    #     if product_obj['units'] == "in":
    #         depth = product_obj['depth'] +'"'+'D'
    #     elif product_obj['units'] == "ft":
    #         depth = product_obj['depth'] +"'"+'D'
    #     elif product_obj['units'] == "mm":
    #         depth = str( product_obj['depth']) +"mm"+'D'
    # if product_obj['length']:
    #     if product_obj['units'] == "in":
    #         length = product_obj['length'] +'"'+'L'
    #     elif product_obj['units'] == "ft":
    #         length = product_obj['length'] +"'"+'L'
    #     elif product_obj['units'] == "mm":
    #         length = str( product_obj['length']) +"mm"+'L'
    product_obj_save = {
        "model" :product_obj['model'],
        "upc_ean" :product_obj['upc_ean'],
        "mpn" :product_obj['mpn'],     
        "breadcrumb":product_obj['breadcrumb'],
        "brand_id":ObjectId(product_obj['brand_id']),
        "product_name":product_obj['product_name'],
        "long_description":product_obj['long_description'],
        "short_description":product_obj['short_description'],
        "features":product_obj['features'],
        "attributes":product_obj['attributes'],
        "tags":product_obj['tags'],
        "features_notes":product_obj['features_notes'],
        "option_str":product_obj['option_str'],
        'units':product_obj['units'],
        'image':product_obj['image'],
        # "msrp":str(product_obj['msrp']),
        # "base_price":str(product_obj['base_price']),
        "key_features":product_obj['key_features'],
        "dimensions":str(product_obj['dimensions'])
    }
    products_obj_1 = DatabaseModel.save_documents(products,product_obj_save)
    # products_obj_1.dimensions = (dimensions(height=height, width=width,depth =depth,length = length ))
    products_obj_1.save()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':ObjectId(category_id),'brand_id':ObjectId(products_obj_1.brand_id.id),'is_active':True})
    cat_retail_price = 1
    if brand_category_price_obj:
        cat_retail_price = brand_category_price_obj.price
    product_varient_list_check = []
    for z in product_obj['varients']:
        retail_price = str(z['retail_price']) 
        # dimensions = str(z['dimensions']) 
        
        if z['sku_number'] in product_varient_list_check:
            data = dict()
            data['status'] = False
            data['error'] = "Varient Already Created"
            return data
        product_varient_obj = DatabaseModel.save_documents(product_varient,{"sku_number":z['sku_number'],"finished_price":str(z['finished_price']),"un_finished_price":str(z['un_finished_price']),"quantity":z['quantity'],'retail_price':retail_price})
        product_varient_list_check .append(z['sku_number'])
        createradial_price_log(product_varient_obj.id,"0",retail_price,user_login_id,client_id)
        logForCreateProductVarient(product_varient_obj.id,user_login_id,"Created",{})
        for i in z['options']:
            product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{"option_name_id":i['option_name_id'],"option_value_id":i['option_value_id']})
            DatabaseModel.update_documents(product_varient.objects,{"id":product_varient_obj.id},{"add_to_set__varient_option_id":product_varient_option_obj.id})
        DatabaseModel.update_documents(products.objects,{"id":products_obj_1.id},{"add_to_set__options":product_varient_obj.id})
    products_obj = DatabaseModel.save_documents(product_category_config,{'product_id':products_obj_1.id,'category_level':category_name,"category_id":category_id})
    logForCreateProduct(products_obj_1.id,user_login_id,"Created",{})
    data = dict()
    data['status'] = True
    return data


#delete
@csrf_exempt
def deleteCategory(request):
    json_req = JSONParser().parse(request)
    id = json_req.get("id")
    category_name = json_req.get("category_name")
    data = dict()
    if category_name == "level-1":
        category_obj = DatabaseModel.get_document(category.objects,{'id':id})
        if len(category_obj.level_one_category_list)>0:
            data['error'] = "level two category is added so category cannot be deleted"
        else:
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
            if product_category_config_obj:
                data['error'] = "product  is added so category cannot be deleted"
            else:
                DatabaseModel.delete_documents(category.objects,{'id':id})

    elif category_name == "level-2":
        category_id = json_req.get("category_id")
        category_obj = DatabaseModel.get_document(level_one_category.objects,{'id':id})
        if len(category_obj.level_two_category_list)>0:
            data['error'] = "level three category is added so category cannot be deleted"
        else:
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
            if product_category_config_obj:
                data['error'] = "product  is added so category cannot be deleted"
            else:
                DatabaseModel.delete_documents(level_one_category.objects,{'id':id})
                DatabaseModel.update_documents(category.objects,{"id":category_id},{'pull__level_one_category_list':id})
    elif category_name == "level-3":
        category_id = json_req.get("category_id")
        category_obj = DatabaseModel.get_document(level_two_category.objects,{'id':id})
        if len(category_obj.level_three_category_list)>0:
            data['error'] = "level four category is added so category cannot be deleted"
        else:
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
            if product_category_config_obj:
                data['error'] = "product  is added so category cannot be deleted"
            else:
                DatabaseModel.delete_documents(level_two_category.objects,{'id':id})
                DatabaseModel.update_documents(category.objects,{"id":category_id},{'pull__level_two_category_list':id})
    elif category_name == "level-4":
        category_id = json_req.get("category_id")
        category_obj = DatabaseModel.get_document(level_three_category.objects,{'id':id})
        if len(category_obj.level_four_category_list)>0:
            data['error'] = "level five category is added so category cannot be deleted"
        else:
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
            if product_category_config_obj:
                data['error'] = "product  is added so category cannot be deleted"
            else:
                DatabaseModel.delete_documents(level_three_category.objects,{'id':id})
                DatabaseModel.update_documents(category.objects,{"id":category_id},{'pull__level_three_category_list':id})
    elif category_name == "level-5":
        category_id = json_req.get("category_id")
        category_obj = DatabaseModel.get_document(level_four_category.objects,{'id':id})
        if len(category_obj.level_five_category_list)>0:
            data['error'] = "level six category is added so category cannot be deleted"
        else:
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
            if product_category_config_obj:
                data['error'] = "product  is added so category cannot be deleted"
            else:
                DatabaseModel.delete_documents(level_four_category.objects,{'id':id})
                DatabaseModel.update_documents(category.objects,{"id":category_id},{'pull__level_four_category_list':id})
    elif category_name == "level-6":
        category_id = json_req.get("category_id")
        product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'category_id':id})
        if product_category_config_obj:
            data['error'] = "product  is added so category cannot be deleted"
        else:
            DatabaseModel.delete_documents(level_five_category.objects,{'id':id})
            DatabaseModel.update_documents(category.objects,{"id":category_id},{'pull__level_five_category_list':id})
    data = dict()
    data['is_deleted'] = True
    return data


#update
@csrf_exempt
def updateCategory(request):
    json_req = JSONParser().parse(request)
    id = json_req.get("id")
    name = json_req.get("name").title()
    category_name = json_req.get("category_name")
    if category_name == "level-1":
        DatabaseModel.update_documents(category.objects,{'id':id},{'name':name})
    elif category_name == "level-2":
        DatabaseModel.update_documents(level_one_category.objects,{'id':id},{'name':name})
    elif category_name == "level-3":
        DatabaseModel.update_documents(level_two_category.objects,{'id':id},{'name':name})
    elif category_name == "level-4":
        DatabaseModel.update_documents(level_three_category.objects,{'id':id},{'name':name})
    elif category_name == "level-5":
        DatabaseModel.update_documents(level_four_category.objects,{'id':id},{'name':name})
    elif category_name == "level-6":
        DatabaseModel.update_documents(level_five_category.objects,{'id':id},{'name':name})
    data = dict()
    data['is_updated'] = True
    return data


def convert_object_ids_to_strings(data):
    if isinstance(data, list):
        for item in data:
            convert_object_ids_to_strings(item)
    elif isinstance(data, dict):
        for key, value in data.items():
            if key == '_id' and isinstance(value, ObjectId):
                data[key] = str(value)
            else:
                convert_object_ids_to_strings(value)


def obtainCategoryAndSections(request):
    client_id = get_current_client()
    pipeline = [
         {
            '$match':{'client_id':ObjectId(client_id)}
        },
    {
        '$lookup': {
            'from': 'level_one_category',
            'localField': 'level_one_category_list',
            'foreignField': '_id',
            'as': 'level_one_category'
        }
    },
    {
        '$lookup': {
            'from': 'level_two_category',
            'localField': 'level_one_category.level_two_category_list',
            'foreignField': '_id',
            'as': 'level_two_category'
        }
    },
    {   
        '$lookup': {
            'from': 'level_three_category',
            'localField': 'level_two_category.level_three_category_list',
            'foreignField': '_id',
            'as': 'level_three_category'
        }
    },
    {
        '$lookup': {
            'from': 'level_four_category',
            'localField': 'level_three_category.level_four_category_list',
            'foreignField': '_id',
            'as': 'level_four_category'
        }
    },
    {
        '$lookup': {
            'from': 'level_five_category',
            'localField': 'level_four_category.level_five_category_list',
            'foreignField': '_id',
            'as': 'level_five_category'
        }
    }]
    flat_result = list(category.objects.aggregate(*pipeline))
    transformed_result = [] 
    for entry in flat_result:  
        category_entry = {
            "_id": entry['_id'],
            "name": entry['name'],
            "level_one_category_list": [] ,
            "level_one_category_count": 0
        }
        level_two_map = {level_two['_id']: level_two for level_two in entry.get('level_two_category', [])}
        for level_one in entry.get('level_one_category', []):
            level_one_entry = {
                "_id": level_one['_id'],
                "name": level_one['name'],
                "level_two_category_list": [] ,
                "level_two_category_count": 0 
            }
            for level_two_id in level_one.get('level_two_category_list', []):
                level_two = level_two_map.get(level_two_id)
                if level_two:
                    level_two_entry = {
                        "_id": level_two['_id'],
                        "name": level_two['name'],
                        "level_three_category_list": [],
                        "level_three_category_count": 0 
                    }
                    level_three_map = {level_three['_id']: level_three for level_three in entry.get('level_three_category', [])}
                    for level_three_id in level_two.get('level_three_category_list', []):
                        level_three = level_three_map.get(level_three_id)
                        if level_three:
                            level_three_entry = {
                                "_id": level_three['_id'],
                                "name": level_three['name'],
                                "level_four_category_list": [] ,
                                "level_four_category_count": 0 
                            }
                            level_four_map = {level_four['_id']: level_four for level_four in entry.get('level_four_category', [])}
                            for level_four_id in level_three.get('level_four_category_list', []):
                                level_four = level_four_map.get(level_four_id)
                                if level_four:
                                    level_four_entry = {
                                        "_id": level_four['_id'],
                                        "name": level_four['name'],
                                        "level_five_category_list": [] ,
                                        "level_five_category_count": 0 
                                    }
                                    level_five_map = {level_five['_id']: level_five for level_five in entry.get('level_five_category', [])}
                                    for level_five_id in level_four.get('level_five_category_list', []):
                                        level_five = level_five_map.get(level_five_id)
                                        if level_five:
                                            level_five_entry = {
                                                "_id": level_five['_id'],
                                                "name": level_five['name']
                                            }
                                            level_four_entry['level_five_category_list'].append(level_five_entry) 
                                    level_three_entry['level_four_category_list'].append(level_four_entry) 
                                    level_four_entry['level_five_category_count'] = len(level_four_entry['level_five_category_list'])
                            level_two_entry['level_three_category_list'].append(level_three_entry) 
                            level_three_entry['level_four_category_count'] = len(level_three_entry['level_four_category_list'])
                    level_one_entry['level_two_category_list'].append(level_two_entry) 
                    level_two_entry['level_three_category_count'] = len(level_two_entry['level_three_category_list'])
            category_entry['level_one_category_list'].append(level_one_entry) 
            level_one_entry['level_two_category_count'] = len(level_one_entry['level_two_category_list'])
        transformed_result.append(category_entry) 
        category_entry['level_one_category_count'] = len(category_entry['level_one_category_list'])
    result = sorted(transformed_result, key=lambda x: x['_id'])
    last_all_ids = []
    category_list = DatabaseModel.list_documents(category.objects,{'client_id':ObjectId(client_id)})
    for category_obj in category_list:
        if len(category_obj.level_one_category_list)>0:
            for i in category_obj.level_one_category_list:
                if len(i.level_two_category_list)>0:
                    for j in i.level_two_category_list:
                        if len(j.level_three_category_list)>0:
                            for k in j.level_three_category_list:
                                if len(k.level_four_category_list)>0:
                                    for l in  k.level_four_category_list:
                                        if len(l.level_five_category_list)>0:
                                            for m in  l.level_five_category_list:
                                                last_all_ids.append({'id':str(m.id),'name':m.name})
                                        else:
                                            last_all_ids.append({'id':str(l.id),'name':l.name})
                                else:
                                    last_all_ids.append({'id':str(k.id),'name':k.name})
                        else:
                            last_all_ids.append({'id':str(j.id),'name':j.name})
                else:
                    last_all_ids.append({'id':str(i.id),'name':i.name})
        else:
            last_all_ids.append({'id':str(category_obj.id),'name':category_obj.name})
    data = dict()
    data['last_level_category'] = last_all_ids
    convert_object_ids_to_strings(result)  
    data['category_list'] = result
    data['category_count'] = len(result)
    return data


@csrf_exempt
def obtainAllProductList(request):
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    print("USER_LGOIN_ID",user_login_id)
    client_id = get_current_client()
    category_id = request.GET.get("category_id")
    varient_option_name = request.GET.get("variant_option_name_id")
    varient_option_value = request.GET.get("variant_option_value_id")
    brand_id = request.GET.get("brand_id")
    filter_param = request.GET.get("filter")
    search_term = request.GET.get('search')
    pg = request.GET.get('pg')
    level_name = request.GET.get("level_name")
    raw_key_string = (
        f"{user_login_id}|{category_id}|{varient_option_name}|"
        f"{varient_option_value}|{brand_id}|{filter_param}|"
        f"{search_term}|{pg}|{level_name}"
    )
    # hashed_key = hashlib.md5(raw_key_string.encode('utf-8')).hexdigest()
    # cache_key = f"prod_list_{hashed_key}"
    # cached_result = cache.get(cache_key)
    # if cached_result:
    #     print(f"Cache HIT: {cache_key}")
    #     return cached_result
    if pg:
        try:
            pg = int(pg)
            from_pg = (pg - 1) * 25
            to_pg = pg * 25
        except ValueError:
            from_pg = 0
            to_pg = 25
    else:
        from_pg = 0
        to_pg = 25
    if search_term == None:
        search_term = ""
    if filter_param == "true" or filter_param == None:
        reverse_check = -1
    else:
        reverse_check = 1
    level_name = request.GET.get("level_name")
    brand_obj = {}
    if brand_id != None:
        brand_obj = {"brand._id":ObjectId(brand_id)}
    product_varient_option_obj = {}
    if varient_option_name:
        varient_option_obj = DatabaseModel.get_document(varient_option.objects,{'id':ObjectId(varient_option_name),'category_str':category_id})
        if varient_option_obj:
            product_varient_option_obj = {'product_varient_option_ins.option_name_id':varient_option_obj.option_name_id.id}
            if varient_option_value:
                product_varient_option_obj = {
        "$and": [
            {"product_varient_option_ins.option_name_id": varient_option_obj.option_name_id.id},
            {"product_varient_option_ins.option_value_id": ObjectId(varient_option_value)}
        ]
        }
    category_obj = {}
    if category_id != None and level_name == None:
        category_obj = {"category_id":category_id}
    
    elif category_id:
        all_ids = []
        if level_name == "level-1":
            category_obj = DatabaseModel.get_document(category.objects,{'id':category_id,"client_id":client_id})
            if category_obj:
                all_ids.append(category_id)
                for i in category_obj.level_one_category_list:
                    all_ids.append(i.id)
                    for j in i.level_two_category_list:
                        all_ids.append(j.id)
                        for k in j.level_three_category_list:
                            all_ids.append(k.id)
                            for l in  k.level_four_category_list:
                                all_ids.append(l.id)
                                for m in  l.level_five_category_list:
                                    all_ids.append(m.id)
        elif  level_name == "level-2":
            level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'id':category_id})
            if level_one_category_obj:
                all_ids.append(level_one_category_obj.id)
                for j in level_one_category_obj.level_two_category_list:
                    all_ids.append(j.id)
                    for k in j.level_three_category_list:
                        all_ids.append(k.id)
                        for l in  k.level_four_category_list:
                            all_ids.append(l.id)
                            for m in  l.level_five_category_list:
                                all_ids.append(m.id)
        elif  level_name == "level-3":
            level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'id':category_id})
            if level_two_category_obj:
                all_ids.append(level_two_category_obj.id)
                for k in level_two_category_obj.level_three_category_list:
                    all_ids.append(k.id)
                    for l in  k.level_four_category_list:
                        all_ids.append(l.id)
                        for m in  l.level_five_category_list:
                            all_ids.append(m.id)
        elif  level_name == "level-4":
            level_three_category_obj = DatabaseModel.get_document(level_three_category.objects,{'id':category_id})
            if level_three_category_obj:
                all_ids.append(level_three_category_obj.id)
                for l in  level_three_category_obj.level_four_category_list:
                    all_ids.append(l.id)
                    for m in  l.level_five_category_list:
                        all_ids.append(m.id)
        elif  level_name == "level-5":
            level_four_category_obj = DatabaseModel.get_document(level_four_category.objects,{'id':category_id})
            if level_four_category_obj:
                all_ids.append(level_four_category_obj.id)
                for m in  level_four_category_obj.level_five_category_list:
                    all_ids.append(m.id)
        all_ids = [str(i) for i in all_ids]
        category_obj = {"category_id":{'$in':all_ids}}
    else:
        category_obj = {}
    user_obj = DatabaseModel.get_document(user.objects,{'id':user_login_id})
    active_obj = {}
    if user_obj.role == 'client-admin':
        active_obj = {'products.is_active':True} 
    pipeline = [
    {
            "$match":category_obj
        },
        {
        '$lookup': {
            'from': 'products',
            'localField': 'product_id',
            'foreignField': '_id',
            'as': 'products'
        }
    }, 
    {
            "$match":active_obj
        },
    {
            '$unwind': {
                'path': '$products',
                # 'preserveNullAndEmptyArrays': True
            }
        },
    # {
    #     "$match":{'products.client_id':ObjectId(client_id)}
    # }
    ]
    if product_varient_option_obj:
        pipeline.extend([
        {
                '$lookup': {
                    "from": 'product_varient',
                    "localField": 'products.options',
                    "foreignField": "_id",
                    "as": "product_varient_ins"
                }
            },
            {
                '$unwind': {
                    'path': '$product_varient_ins',
                    'preserveNullAndEmptyArrays': True
                }
            }, 
            {
                '$lookup': {
                    "from": 'product_varient_option',
                    "localField": 'product_varient_ins.varient_option_id',
                    "foreignField": "_id",
                    "as": "product_varient_option_ins"
                }
            },
            {
                '$unwind': {
                    'path': '$product_varient_option_ins',
                    'preserveNullAndEmptyArrays': True
                }
            } ,{
                "$match": product_varient_option_obj
            }])
    pipeline.extend([
    {
        '$lookup': {
            'from': 'brand',
            'localField': 'products.brand_id',
            'foreignField': '_id',
            'as': 'brand'
        }
    }, 
    {
            '$unwind': {
                'path': '$brand',
                'preserveNullAndEmptyArrays': True
            }
        },{
            "$match":brand_obj
        },
    {
        '$group': {
            "_id":'$_id',
            # 'product_list': {
            #     "$push": {
            'product_name':{'$first': "$products.product_name"},
            'product_id':{'$first': "$products._id"},
            'model':{'$first':"$products.model"},
            'upc_ean':{'$first':"$products.upc_ean"},
            'is_active':{'$first':"$products.is_active"},
            'breadcrumb':{'$first':"$products.breadcrumb"},
            'brand':{'$first':"$brand.name"},
            'long_description':{'$first':"$products.long_description"},
            'short_description':{'$first':"$products.short_description"},
            'features':{'$first':"$products.features"},
            'attributes':{'$first':"$products.attributes"},
            'tags':{'$first':"$products.tags"},
            'msrp':{'$first':"$products.msrp"},
            'mpn':{'$first':"$products.mpn"},
            'base_price':{'$first':"$products.base_price"},
            'key_features':{'$first':"$products.key_features"},
            'image':{'$first':"$products.image"},
            'level':{'$first':'$category_level'},
            'category_id':{'$first':'$category_id'}
                }
            },{
    '$match': {
    '$or': [
        { 'upc_ean': { '$regex': search_term, '$options': 'i' } },  
        { 'short_description': { '$regex': search_term, '$options': 'i' } }, 
        { 'mpn': { '$regex': search_term, '$options': 'i' } },  
        { 'product_name': { '$regex': search_term, '$options': 'i' } },
        { 'brand': { '$regex': search_term, '$options': 'i' } }, 
        { 'model': { '$regex': search_term, '$options': 'i' } },
        { 'features': { '$regex': search_term, '$options': 'i' } },
]
    }
  },{'$sort': {'_id':reverse_check}}
        # }
    # }
    ])
    result = list(product_category_config.objects.aggregate(*pipeline))
    data = dict()
    data['product_count'] = len(result)
    data['product_id_list'] = [str(i['product_id'])for i in result]
    result_ = result[from_pg:to_pg]
    # result = result[:25]
    for j in result_:
        del (j['_id'])
        j['product_id'] = str(j['product_id']) if 'product_id'in j else ""
        getCategoryLevelOrder(j)
    data['product_list'] = result_
    # data['product_list'] = sorted(data['product_list'], key=lambda x: ObjectId(x['product_id']),reverse=reverse_check)
    return data


@csrf_exempt
def obtainProductDetails(request):
    json_req = JSONParser().parse(request)
    product_id = ObjectId(json_req['id'])
    pipeline = [
    {
            "$match":{'_id':product_id}
        }, {
        '$lookup': {
            'from': 'brand',
            'localField': 'brand_id',
            'foreignField': '_id',
            'as': 'brand'
        }
    }, 
    {
            '$unwind': {
                'path': '$brand',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
        '$group': {
            "_id":None,
            'product_obj': {
                "$first": {
                    'product_name': "$product_name",
                    'product_id': "$_id",
                    'mpn': "$mpn",
                    'model':"$model",
                    'upc_ean':"$upc_ean",
                    'breadcrumb':"$breadcrumb",
                    'brand':"$brand.name",
                    'brand_id':"$brand._id",
                    'product_name':"$product_name",
                    'long_description':"$long_description",
                    'short_description':"$short_description",
                    'features':"$features",
                    'attributes':"$attributes",
                    'tags':"$tags",
                    'features_notes':"$features_notes",
                    'option_str':"$option_str",
                    'dimensions':"$dimensions",
                    'units':'$units',
                    'msrp':"$msrp",
                    'base_price':"$base_price",
                    'key_features':"$key_features",
                    'image':"$image",
                }
            }
        }
    }
    ]
    result = list(products.objects.aggregate(*pipeline))
    if len(result)>0:
        result = result[0]
        del result['_id']
        result['product_obj']['product_id'] = str(result['product_obj']['product_id'])
        result['product_obj']['brand_id'] = str(result['product_obj']['brand_id'])
        pipeline = [
            {
                "$match": {
                    "product_id": ObjectId(result['product_obj']['product_id'])
                }
            },
            {
                "$project": {
                    "category_id": 1,
                    "category_level": 1
                }
            },{ "$limit": 1 }
        ]

        product_category_config_obj = list(product_category_config.objects.aggregate(pipeline))
        if product_category_config_obj:
            result['category_id'] = str(product_category_config_obj[0]['category_id'])
            result['category_name'] = product_category_config_obj[0]['category_level']
        i = dict()
        i['level'] = result['category_name']
        i['category_id'] = result['category_id']
        result['category_brand_price'] = dict()
        pipeline = [
            {
                "$match": {
                    "category_id": str(i['category_id']),
                    "brand_id": ObjectId(result['product_obj']['brand_id']),
                    "is_active": True
                }
            },
            {
                "$project": {
                    "price": 1,
                    "price_option": 1
                }
            },
            { "$limit": 1 }
        ]

        brand_category_price_obj = list(brand_category_price.objects.aggregate(pipeline))

        if brand_category_price_obj:
            result['category_brand_price']['price'] = brand_category_price_obj[0]['price']
            result['category_brand_price']['price_option'] = brand_category_price_obj[0]['price_option']

        else:
            result['category_brand_price']['price'] = 1
            result['category_brand_price']['price_option'] = "finished_price"
        getCategoryLevelOrder(i)
        result['category_level'] = i['category_name']
    return  result


def productBulkUpdate(request):
    json_req = JSONParser().parse(request)
    product_obj_list = json_req['product_obj_list']
    for i in product_obj_list:
        DatabaseModel.update_documents(products.objects,{'id':i['id']},i['update_obj'])
    data = dict()
    data['is_updated'] = True  
    return data


@csrf_exempt
def productUpdate(request):
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    product_id = json_req['id']
    json_req['update_obj']['brand_id'] = ObjectId(json_req['update_obj']['brand_id'])
    # height = json_req['update_obj']['height']
    # del json_req['update_obj']['height']
    # width = json_req['update_obj']['width']
    # del json_req['update_obj']['width']
    # depth = json_req['update_obj']['depth']
    # del json_req['update_obj']['depth']
    # length = json_req['update_obj']['length']
    # del json_req['update_obj']['length']
    # length = length.replace("'", '"')
    # if height:
    #     height = re.findall(r'\d+', height)
    #     if height:
    #         height = height[0]
    #     else:
    #         height = ""
    #     if json_req['update_obj']['units'] == "in":
    #         height = str(height) +'"'+'H'
    #     elif json_req['update_obj']['units'] == "ft":
    #         height = str(height) +"'"+'H'
    #     elif json_req['update_obj']['units'] == "mm":
    #         height = str(height) +"mm"+'H'
    # if width:
    #     width = re.findall(r'\d+', width)
    #     if width:
    #         width = width[0]
    #     else:
    #         width = ""
    #     if json_req['update_obj']['units'] == "in":
    #         width = str( width) +'"'+'W'
    #     elif json_req['update_obj']['units'] == "ft":
    #         width = str( width) +"'"+'W'
    #     elif json_req['update_obj']['units'] == "mm":
    #         width = str( width) +"mm"+'W'
    # if depth:
    #     depth = re.findall(r'\d+', depth)
    #     if depth:
    #         depth = depth[0]
    #     else:
    #         depth = ""
    #     if json_req['update_obj']['units'] == "in":
    #         depth = depth +'"'+'D'
    #     elif json_req['update_obj']['units'] == "ft":
    #         depth = depth +"'"+'D'
    #     elif json_req['update_obj']['units'] == "mm":
    #         depth = str( depth) +"mm"+'D'
    # if length:
    #     length = re.findall(r'\d+', length)
    #     if length:
    #         length = length[0]
    #     else:
    #         length = ""
    #     if json_req['update_obj']['units'] == "in":
    #         length = length +'"'+'L'
    #     elif json_req['update_obj']['units'] == "ft":
    #         length = length +"'"+'L'
    #     elif json_req['update_obj']['units'] == "mm":
    #         length = str( length) +"mm"+'L'

    update_dict = json_req['update_obj'].copy()
    if 'brand_id' in json_req['update_obj'] and json_req['update_obj']['brand_id']:
        json_req['update_obj']['brand_id'] = ObjectId(json_req['update_obj']['brand_id'])
    DatabaseModel.update_documents(products.objects, {'id': product_id}, json_req['update_obj'])
    products_obj = DatabaseModel.get_document(products.objects,{'id':product_id})
    # products_obj.dimensions = (dimensions(height=height, width=width,depth =depth,length =length))
    products_obj.save()
    logForCreateProduct(product_id,user_login_id,"Updated",update_dict)
    data = dict()
    data['is_updated'] = True
    return data


@csrf_exempt
def varientUpdate(request):
    json_req = JSONParser().parse(request)
    varient_obj = json_req
    product_varient_option_list = list()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    client_id = get_current_client()
    product_varient_obj = DatabaseModel.get_document(product_varient.objects,{'id':varient_obj['id']})
    old_one = ""
    if product_varient_obj:
        old_one = product_varient_obj.retail_price
    for i in varient_obj["options"]: 
        product_varient_option_obj = DatabaseModel.get_document(product_varient_option.objects,{'option_name_id':ObjectId(i['option_name_id']),'option_value_id':ObjectId(i['option_value_id'])})
        if product_varient_option_obj==None:
            product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{'option_name_id':ObjectId(i['option_name_id']),'option_value_id':ObjectId(i['option_value_id'])})
        product_varient_option_list.append(product_varient_option_obj.id)
    x,values_dict = DatabaseModel.update_documents(product_varient.objects,{'id':varient_obj['id']},{'sku_number':varient_obj["sku"],"finished_price":str(varient_obj["finishedPrice"]),"un_finished_price":str(varient_obj["unfinishedPrice"]),"quantity":str(varient_obj["quantity"]),"retail_price":str(varient_obj["retailPrice"]),"varient_option_id":product_varient_option_list})
    createradial_price_log(varient_obj['id'],old_one,str(varient_obj["retailPrice"]),user_login_id,client_id)
    logForCreateProductVarient(varient_obj['id'],user_login_id,"Updated",values_dict)
    data = dict()
    data['is_updated'] = True
    return data

from collections import defaultdict
@csrf_exempt
def obtainAllVarientList(request):
    json_req = JSONParser().parse(request)
    product_id = ObjectId(json_req['product_id'])
    varient_option_name = json_req.get("variant_option_name_id", [])
    varient_option_value = json_req.get("variant_option_value_id", [])

    # Group filter values by option name (group-wise OR)
    grouped_filters = defaultdict(set)
    if varient_option_name and varient_option_value:
        for name_id, value_id in zip(varient_option_name, varient_option_value):
            grouped_filters[ObjectId(name_id)].add(ObjectId(value_id))

    # Step 1: For each option group, find matching variant IDs (OR in group)
    variant_id_sets = []

    for option_name_id, value_ids in grouped_filters.items():
        pipeline = [
            {"$match": {"_id": product_id}},
            {
                "$lookup": {
                    "from": "product_varient",
                    "localField": "options",
                    "foreignField": "_id",
                    "as": "product_varient_ins"
                }
            },
            {"$unwind": "$product_varient_ins"},
            {
                "$lookup": {
                    "from": "product_varient_option",
                    "localField": "product_varient_ins.varient_option_id",
                    "foreignField": "_id",
                    "as": "product_varient_option_ins"
                }
            },
            {"$unwind": "$product_varient_option_ins"},
            {
                "$match": {
                    "product_varient_option_ins.option_name_id": option_name_id,
                    "product_varient_option_ins.option_value_id": {"$in": list(value_ids)}
                }
            },
            {
                "$group": {
                    "_id": "$product_varient_ins._id"
                }
            }
        ]

        results = list(products.objects.aggregate(*pipeline))
        variant_ids = set([r['_id'] for r in results])
        variant_id_sets.append(variant_ids)

    # Step 2: Intersect variant IDs across all option groups (AND across groups)
    if variant_id_sets:
        matching_variant_ids = set.intersection(*variant_id_sets)
    else:
        matching_variant_ids = None  # No filters applied

    # Step 3: Fetch final variant data using matching_variant_ids (if any)
    pipeline = [
        {"$match": {"_id": product_id}},
        {
            "$lookup": {
                "from": "product_varient",
                "localField": "options",
                "foreignField": "_id",
                "as": "product_varient_ins"
            }
        },
        {"$unwind": "$product_varient_ins"},
    ]

    if matching_variant_ids is not None:
        pipeline.append({"$match": {"product_varient_ins._id": {"$in": list(matching_variant_ids)}}})

    pipeline += [
        {
            "$lookup": {
                "from": "product_varient_option",
                "localField": "product_varient_ins.varient_option_id",
                "foreignField": "_id",
                "as": "product_varient_option_ins"
            }
        },
        {"$unwind": "$product_varient_option_ins"},
        {
            "$lookup": {
                "from": "type_name",
                "localField": "product_varient_option_ins.option_name_id",
                "foreignField": "_id",
                "as": "type_name"
            }
        },
        {"$unwind": {"path": "$type_name", "preserveNullAndEmptyArrays": True}},
        {
            "$lookup": {
                "from": "type_value",
                "localField": "product_varient_option_ins.option_value_id",
                "foreignField": "_id",
                "as": "type_value"
            }
        },
        {"$unwind": {"path": "$type_value", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$product_varient_ins._id",
                "id": {"$first": "$product_varient_ins._id"},
                "sku_number": {"$first": "$product_varient_ins.sku_number"},
                "is_active": {"$first": "$product_varient_ins.is_active"},
                "finished_price": {"$first": "$product_varient_ins.finished_price"},
                "un_finished_price": {"$first": "$product_varient_ins.un_finished_price"},
                "retail_price": {"$first": "$product_varient_ins.retail_price"},
                "quantity": {"$first": "$product_varient_ins.quantity"},
                "image_url": {"$first": "$product_varient_ins.image_url"},
                "varient_option_list": {
                    "$push": {
                        "type_name": "$type_name.name",
                        "type_id": "$type_name._id",
                        "type_value": "$type_value.name",
                        "type_value_id": "$type_value._id",
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "id": 1,
                "sku_number": 1,
                "is_active": 1,
                "retail_price": 1,
                "finished_price": 1,
                "un_finished_price": 1,
                "quantity": 1,
                "image_url": 1,
                "varient_option_list": 1
            }
        },
        {"$sort": {"id": 1}}
    ]

    result = list(products.objects.aggregate(*pipeline))

    # Convert ObjectIds to strings for frontend
    for item in result:
        item['id'] = str(item['id'])
        for option in item.get('varient_option_list', []):
            if 'type_id' in option:
                option['type_id'] = str(option['type_id'])
            if 'type_value_id' in option:
                option['type_value_id'] = str(option['type_value_id'])

    return result



@csrf_exempt
def exportAll(request):
    category_id = request.GET.get('category_id')
    client_id = get_current_client()
    is_active_product = request.GET.get('is_active_product')
    is_active_variant = request.GET.get('is_active_variant')
    print("\n" + "="*60)
    print("EXPORT ALL CALLED")
    print(f"category_id: {category_id}")
    print(f"client_id: {client_id}")
    print(f"is_active_product: {is_active_product}")
    print(f"is_active_variant: {is_active_variant}")
    print("="*60)
    if is_active_product == 'true':
        is_active_product = {'is_active': True}
    else:

        is_active_product = {}
    if is_active_variant == 'true':

        is_active_variant = {'product_varient_ins.is_active': True}
    else:

        is_active_variant = {}
    if category_id:
        category_obj = {'product_category_config_ins.category_id':category_id,"client_id":ObjectId(client_id)}
    else:

        category_obj = {"client_id":ObjectId(client_id)}
    pipeline = [
        {
            '$match':is_active_product
        },
        {
            '$lookup': {
                "from": 'product_varient',
                "localField": 'options',
                "foreignField": "_id",
                "as": "product_varient_ins"
            }
        },
        {'$unwind': {'path': '$product_varient_ins', 'preserveNullAndEmptyArrays': True}},
          {
            '$match':is_active_variant
        },
        {
            '$lookup': {
                "from": 'product_category_config',
                "localField": '_id',
                "foreignField": "product_id",
                "as": "product_category_config_ins"
            }
        },
        {'$unwind': {'path': '$product_category_config_ins', 'preserveNullAndEmptyArrays': True}},
        {
            "$match": category_obj
        },
        {
            '$lookup': {
                'from': 'product_varient_option',
                'localField': 'product_varient_ins.varient_option_id',
                'foreignField': '_id',
                'as': 'product_varient_option_ins'
            }
        },  
        # {'$unwind': {'path': '$product_varient_option_ins', 'preserveNullAndEmptyArrays': True}},{
        # '$lookup': {
        #     'from': 'product_varient_option',
        #     'localField': 'product_varient_ins.varient_option_id',
        #     'foreignField': '_id',
        #     'as': 'product_varient_option_ins'
        # }
        # },
        {
            '$unwind': {
                'path': '$product_varient_option_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, {
        '$lookup': {
            'from': 'type_name',
            'localField': 'product_varient_option_ins.option_name_id',
            'foreignField': '_id',
            'as': 'type_name'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_name',
                'preserveNullAndEmptyArrays': True
            }
        },    {
        '$lookup': {
            'from': 'type_value',
            'localField': 'product_varient_option_ins.option_value_id',
            'foreignField': '_id',
            'as': 'type_value'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_value',
                'preserveNullAndEmptyArrays': True
            }
        }, {
        '$lookup': {
            'from': 'brand',
            'localField': 'brand_id',
            'foreignField': '_id',
            'as': 'brand'
        }
    }, 
    {
            '$unwind': {
                'path': '$brand',
                'preserveNullAndEmptyArrays': True
            }
        },{
            "$group": {
                "_id": "$product_varient_ins._id",
                "model":{ "$first":"$model"},
                "mpn":{ "$first":"$mpn"},
                "upc_ean":{ "$first":"$upc_ean"},
                "product_name":{ "$first":"$product_name"},
                "product_id":{ "$first":"$_id"},
                "category level":{ "$first":"$product_category_config_ins.category_level"},
                "category_id":{ "$first":"$product_category_config_ins.category_id"},
                "long_description":{ "$first":"$long_description"},
                "short_description":{ "$first":"$short_description"}, 
                "brand":{ "$first":"$brand.name"},
                "breadcrumb":{ "$first":"$breadcrumb"},
                "option_str":{ "$first":"$option_str"},
                "is_active":{ "$first":"$is_active"},
                "retail_price":{ "$first":"$product_varient_ins.retail_price"},
                "Tags":{ "$first":"$tags"}, 
                "dimensions":{ "$first":"$dimensions"}, 
                "Variant SKU":{ "$first":"$product_varient_ins.sku_number"},
                "Un Finished Price":{ "$first":"$product_varient_ins.un_finished_price"},
                "Finished Price":{ "$first":"$product_varient_ins.finished_price"},
                "Image Src":{ "$first":"$image"},
                "Key Features":{ "$first":"$key_features"},
                "stockv":{ "$first":"$product_varient_ins.quantity"},
                "varient_option_list":{'$addToSet':{'name':"$type_name.name","value":"$type_value.name",'product_varient_option_id':'$product_varient_option_ins._id'}}
        }
    } , {
            '$project': {
                "id": "$_id",
                "_id": 0,
                "model":1,
                "upc_ean":1,
                "mpn":1,
                "category level":1,
                "category_id":1,
                "product_name":1,
                "long_description":1,
                "short_description":1, 
                "brand":1,
                "breadcrumb":1,
                "product_id":1,
                "retail_price":1,
                "Tags":1, 
                "dimensions":1, 
                "Variant SKU":1,
                "Un Finished Price":1,
                "Finished Price":1,
                "Image Src":1, 
                "Image Position":1,
                "Key Features":1,
                "stockv":1,
                "varient_option_list":1,
                'mpn':1,
                "option_str":1,
                'is_active':1
            }
        },{
            '$sort':{'id':1}
        }
    
    ]
    print("\nRUNNING AGGREGATION PIPELINE...")
    result = list(products.objects.aggregate(*pipeline))
    print(f"AGGREGATION RESULT COUNT: {len(result)} document(s)")
    if len(result) == 0:
        print("NO PRODUCTS FOUND — WILL RETURN EXCEL WITH ONLY HEADERS")
    else:
        print("SAMPLE OF FIRST RESULT:")
        print(result[0] if result else "None")
    max_variants = 0
    max_image = 0
    
    for i in result:
        if max_variants < len(i['varient_option_list']):
            max_variants = len(i['varient_option_list'])
        if i['Image Src'] != None:
            if max_image < len(i['Image Src']):
                max_image = len(i['Image Src'])
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Products"
    print(f"\nMAX VARIANTS FOUND: {max_variants}")
    print(f"MAX IMAGES PER VARIANT: {max_image}")
    # headers = [   
    # "S.No","mpn", "Variant SKU","Product Name","Model", "UPC/EAN","taxonomy","Brand", "Short Description","Long Description",
    # "Retail Price", "Unfinished Price", "Finished Price"
    # ]
    headers = ["Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags","Published",]
    variant_headers = []
    for i in range(1, max_variants + 1):
        variant_headers.append(f"Option{i} Name")
        variant_headers.append(f"Option{i} Value")
        variant_headers.append(f"Option{i} Linked To")
    headers.extend(variant_headers)
    headers.extend(["Variant SKU","Variant Grams","Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy","Variant Fulfillment Service","Variant Price","Variant Compare At Price","Variant Requires Shipping","Variant Taxable","Variant Barcode","Image Src","Image Position","Image Alt Text","Gift Card","SEO Title","SEO Description","Google Shopping / Google Product Category","Google Shopping / Gender","Google Shopping / Age Group","Google Shopping / MPN","Google Shopping / Condition","Google Shopping / Custom Product","Google Shopping / Custom Label 0","Google Shopping / Custom Label 1","Google Shopping / Custom Label 2","Google Shopping / Custom Label 3","Google Shopping / Custom Label 4","Complimentary Product Text (product.metafields.custom.complimentary_product_text)","Custom Additions (product.metafields.custom.custom_additions)","Details as Shown (product.metafields.custom.details_as_shown1)","Dimensions (product.metafields.custom.dimensions1)","Key Features (product.metafields.custom.key_features1)","Options (product.metafields.custom.options1)","Quickship or Regular (product.metafields.custom.quickship_or_regular)","Quickship or Regular Product (product.metafields.custom.quickship_or_regular_product)","Google: Custom Product (product.metafields.mm-google-shopping.custom_product)","Bed/Frame features (product.metafields.shopify.bed-frame-features)","Color (product.metafields.shopify.color-pattern)","Frame color (product.metafields.shopify.frame-color)","Furniture/Fixture features (product.metafields.shopify.furniture-fixture-features)","Furniture/Fixture material (product.metafields.shopify.furniture-fixture-material)","Seat structure (product.metafields.shopify.seat-structure)","Tabletop shape (product.metafields.shopify.tabletop-shape)","Upholstery material (product.metafields.shopify.upholstery-material)","Complementary products (product.metafields.shopify--discovery--product_recommendation.complementary_products)","Related products (product.metafields.shopify--discovery--product_recommendation.related_products)","Related products settings (product.metafields.shopify--discovery--product_recommendation.related_products_display","Variant Image","Variant Weight Unit","Variant Tax Code","Cost per item","Included / United States","Price / United States","Compare At Price / United States","Status"])

    worksheet.append(headers)
    def generate_shopify_handle(product_name):
        name = product_name.lower()
        name = re.sub(r'[^a-z0-9\s-]', '', name)  # remove special characters
        name = re.sub(r'\s+', '-', name)          # replace spaces with hyphens
        name = re.sub(r'-+', '-', name)           # replace multiple hyphens with one
        return name.strip('-')   
    title_list_dublicate = dict()
    for i, item in enumerate(result):
        i_dict = dict()
        i_dict['level'] = item.get("category level", "")
        i_dict['category_id'] = item.get("category_id", "")
        try:
            dimensions = item.get("dimensions", "")
        except:
            dimensions = ""
        getCategoryLevelOrder(i_dict)
        if item.get("is_active") == True:
            status = "active"
        else:
            status = "draft"
        variant_options = sorted(
                item.get("varient_option_list", []),
                key=lambda x: ObjectId(x.get("product_varient_option_id", "0"*24))
            )
        if item.get("product_name", "") in title_list_dublicate:
            title_releated_details = {'title':"",'body':"",'vendor':"",'cat':"",'category_last_name':"", "tags":"",'published':"",'count':title_list_dublicate[item.get("product_name", "")]['count'] +1,'status':"",'dimensions':"",'key_features':"",'option_str':"",'Gift_Card':""}
            for j in range(max_variants):
                if j < len(variant_options):
                    title_releated_details[f'option{j+1} Name'] = ""
            title_list_dublicate[item.get("product_name", "")]['count'] +=1
        else:
            
            title_releated_details = {'title':item.get("product_name", ""),'body':item.get("long_description", ""),'vendor':item.get("brand", ""),'cat':i_dict.get("category_name", ""),'category_last_name':i_dict.get("category_last_name",""), "tags":item.get("Tags", ""),'published':"TRUE",'count':0,'status':status,'dimensions':dimensions,'key_features':item.get("Key Features", ""),'option_str':item.get("option_str", ""),'Gift_Card':'FALSE'}
            title_list_dublicate[item.get("product_name", "")] = title_releated_details
            for j in range(max_variants):
                if j < len(variant_options):
                    title_releated_details[f'option{j+1} Name'] = variant_options[j].get('name', '')
        row = [
            generate_shopify_handle(item.get("product_name", "")), #handle
            title_releated_details.get('title'),
            title_releated_details.get('body'),
            title_releated_details.get('vendor'),
            title_releated_details.get('cat'),
            title_releated_details.get('category_last_name'),
            title_releated_details.get('tags'),
            title_releated_details.get('published'),
        ]
        
        for j in range(max_variants):
            if j < len(variant_options):
                row.append(title_releated_details[f'option{j+1} Name'])
                val = variant_options[j].get('value', '')
                if val is None or str(val).lower() == 'nan' or (isinstance(val, float) and math.isnan(val)):
                    val = ''
                row.append(val)
                row.append(variant_options[j].get(''))
            else:
                row.append('')  # Add empty values for missing variants
                row.append('')  # Add empty values for missing variants
                row.append('')  # Add empty values for missing variants
        retail_price = item.get("retail_price",0.0)
        if retail_price != 'None' and retail_price != None and retail_price != "":
            retail_price = str(round(float(retail_price),2))
        row.append(item.get("Variant SKU", ""))
        row.extend(["","shopify","0","continue","manual",retail_price,"","TRUE","TRUE",""])
        img_src = item.get("Image Src", [])
        if img_src == "" or img_src ==None:
            img_src = []
            image_pos = ""

        else:
        # img_src_str = ""
        # for j in img_src:
        #     if j != "":
        #         img_src_str = str(j) +"," + str(img_src_str)
            try:
                row.append(img_src[title_releated_details['count']])  
                image_pos = title_releated_details['count']+1
            except:
                row.append("")  
                image_pos = ""
        row.extend([image_pos,"",title_releated_details['Gift_Card'],"","","","","","","","","","","","","","","","",title_releated_details['dimensions'],title_releated_details['key_features'],title_releated_details['option_str'],"","","","","","","","","","","","","","","","lb",""])
        f_p = item.get("Finished Price",0.0)
        if f_p != 'None' and f_p != None and f_p != "":
            f_p = str(round(float(f_p),2))
        row.append(f_p)
        
        row.extend(["","","",title_releated_details['status']])
        worksheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0) 
    response = HttpResponse(buffer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = 'attachment; filename="products.xlsx"'
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    print("CONTENT LENGTH:", len(response.content))
    print("FIRST 500 BYTES:", response.content[:500])
    print("HAS EXCEL MAGIC BYTES?", response.content.startswith(b'PK'))  #
    return response


@csrf_exempt
def retrieveData(request):
    data = dict()
    data['status'] = False
    if 'file' not in request.FILES:
        return data
    file = request.FILES['file']
    try:
        if file.name.endswith('.xlsx'):
            sheets_dict = pd.read_excel(file, sheet_name=None)
            all_sheets_json = {}
            for sheet_name, df in sheets_dict.items():
                json_data = df.to_json(orient='records')
                all_sheets_json[sheet_name] = json_data

            with open('sheets_output.json', 'w') as json_file:
                json.dump(all_sheets_json, json_file)
        elif file.name.endswith('.csv'):
            df = pd.read_csv(file)
            json_data = df.to_json(orient='records')
        elif file.name.endswith('.txt'):
            df = pd.read_csv(file, sep='\t') 
            json_data = df.to_json(orient='records')
        elif file.name.endswith('.pdf'):
            with pdfplumber.open(file) as pdf:
                all_text = ""
                for page in pdf.pages:
                    all_text += page.extract_text()
        else:
            return data
    except Exception as e:
        return data


@csrf_exempt
def obtainVarientForCategory(request):
    category_id = request.GET.get("id")
    client_id = get_current_client()
    pipeline = [
        {
            "$match":{'category_id':category_id}
        },
        {
        '$lookup': {
            'from': 'varient_option',
            'localField': 'varient_option_id_list',
            'foreignField': '_id',
            'as': 'varient_option'
        }
        }, 
        {
            '$unwind': {
                'path': '$varient_option',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            "$match":{'varient_option.client_id':ObjectId(client_id)}
        },
        {
        '$lookup': {
            'from': 'type_name',
            'localField': 'varient_option.option_name_id',
            'foreignField': '_id',
            'as': 'type_name'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_name',
                'preserveNullAndEmptyArrays': True
            }
        },    {
        '$lookup': {
            'from': 'type_value',
            'localField': 'varient_option.option_value_id_list',
            'foreignField': '_id',
            'as': 'type_value'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_value',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
        '$group': {
            "_id":"$varient_option",
            "type_name":{'$first':"$type_name.name"},
            "varient_option_id":{'$first':"$varient_option._id"},
            "type_id":{'$first':"$type_name._id"},
            "category_varient_id":{'$first':"$_id"},
            'option_value_list': {
                "$push": {
                    'type_value_name': "$type_value.name",
                    'type_value_id': "$type_value._id",
                }
            }
        }
        },{
        '$project':{
            "_id":0,
            "type_name":1,
            'option_value_list': 1,
            'varient_option_id':1,
            'category_varient_id':1,
            'type_id':1
        }
        },{
            '$sort':{'type_id':1}
        }
        ]
    result = list(category_varient.objects.aggregate(*pipeline))
    
    data = dict()
    data['category_varient_id'] = ""
    if len(result)>0:
        for i in result:
            i['type_id'] = str(i['type_id']) if 'type_id'in i else ""
            data['category_varient_id'] = str(i['category_varient_id'])
            del i['category_varient_id']
            i ['tagged_category_list'] = list()
            category_varient_list = DatabaseModel.list_documents(category_varient.objects,{'varient_option_id_list__in':[ i['varient_option_id']]})
            for ins in category_varient_list:
                dist_i = dict()
                dist_i['level'] = ins.category_level
                dist_i['category_id'] = ins.category_id
                dist_i['category_name'] = ""
                getCategoryLevelOrder(dist_i)
                i ['tagged_category_list'].append(dist_i['category_name'])
            i['varient_option_id'] =  str(i['varient_option_id'])
            try:
                for j in i['option_value_list']:
                    j['type_value_id'] = str(j['type_value_id']) 
            except:
                i['option_value_list'] = []
    data['varient_list'] = result
    return data


@csrf_exempt
def createVarientOption(request):
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    client_id = get_current_client()
    name = json_req.get("name").title()
    category_varient_id = json_req.get("category_varient_id")
    category_id = json_req.get("category_id")
    category_level = json_req.get("category_name")
    if category_varient_id == "":
        category_varient_obj = DatabaseModel.save_documents(category_varient,{'category_id':category_id,'category_level':category_level})
        category_varient_id = str(category_varient_obj.id)
    type_name_obj = DatabaseModel.get_document(type_name.objects,{'name':name})
    varient_option_obj_check_id = []
    if type_name_obj:
        type_name_id = type_name_obj.id
        varient_option_obj_check = DatabaseModel.get_document(varient_option.objects,{'option_name_id':type_name_id,'client_id':ObjectId(client_id),'category_str':category_id})
        if varient_option_obj_check:
            varient_option_obj_check_id = [varient_option_obj_check.id]
            varient_option_id = varient_option_obj_check.id
        else:
            varient_option_obj_check = DatabaseModel.save_documents(varient_option,{'option_name_id':type_name_id,'client_id':ObjectId(client_id),'category_str':category_id,'category_str':category_id})
            varient_option_obj_check_id = [varient_option_obj_check.id]
            varient_option_id = varient_option_obj_check.id
        # else:
        # varient_option_obj = DatabaseModel.save_documents(varient_option,{'option_name_id':type_name_id})
        # varient_option_id = varient_option_obj.id
    else:
        type_name_id = DatabaseModel.save_documents(type_name,{'name':name})
        varient_option_obj_check = DatabaseModel.get_document(varient_option.objects,{'option_name_id':type_name_id,'client_id':ObjectId(client_id),'category_str':category_id})
        if varient_option_obj_check:
            varient_option_obj_check_id = [varient_option_obj_check.id]
            varient_option_id = varient_option_obj_check.id
        else:
            varient_option_obj_check = DatabaseModel.save_documents(varient_option,{'option_name_id':type_name_id,'client_id':ObjectId(client_id),'category_str':category_id,'category_str':category_id})
            varient_option_obj_check_id = [varient_option_obj_check.id]
            varient_option_id = varient_option_obj_check.id
    category_varient_obj = DatabaseModel.get_document(category_varient.objects,{"id":category_varient_id,'category_id':category_id,'varient_option_id_list__in':varient_option_obj_check_id})
    if category_varient_obj:
        data = dict()
        data['is_created'] = False
        data['error'] = "Variant already Exists"
        return data
    
    obtainlogForCategoryVarientOption(category_id,varient_option_id,"create",ObjectId(user_login_id),category_level,{})
    DatabaseModel.update_documents(category_varient.objects,{"id":category_varient_id},{'add_to_set__varient_option_id_list':varient_option_id})
    
    data = dict()
    data['is_created'] = True
    return data


@csrf_exempt
def createValueForVarientName(request):
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    data = dict()
    client_id = get_current_client()
    option_id = json_req.get("option_id")
    varient_option_id	 = json_req.get("varient_option_id")
    type_value_obj = DatabaseModel.get_document(type_value.objects,{'name':name})
    if type_value_obj:
        type_value_id = type_value_obj.id
    else:
        type_value_id = DatabaseModel.save_documents(type_value,{'name':name}).id
    varient_option_obj = DatabaseModel.get_document(varient_option.objects,{'id':varient_option_id,"option_name_id":option_id,"client_id":ObjectId(client_id),'option_value_id_list__in':[ObjectId(type_value_id)]})
    if varient_option_obj:
        data['is_created'] = False
        data['error'] = "Variant Value already exists"
    else:
        DatabaseModel.update_documents(varient_option.objects,{"id":varient_option_id,"option_name_id":option_id,'client_id':client_id},{'add_to_set__option_value_id_list':type_value_id})
        data['is_created'] = True
    return data
def traverse_categories(category, result_list):
    """
    Recursively traverse categories and add their IDs and names to the result list.
    """
    if hasattr(category, 'level_five_category_list') and category.level_five_category_list:
        for sub_category in category.level_five_category_list:
            traverse_categories(sub_category, result_list)
    elif hasattr(category, 'level_four_category_list') and category.level_four_category_list:
        for sub_category in category.level_four_category_list:
            traverse_categories(sub_category, result_list)
    elif hasattr(category, 'level_three_category_list') and category.level_three_category_list:
        for sub_category in category.level_three_category_list:
            traverse_categories(sub_category, result_list)
    elif hasattr(category, 'level_two_category_list') and category.level_two_category_list:
        for sub_category in category.level_two_category_list:
            traverse_categories(sub_category, result_list)
    elif hasattr(category, 'level_one_category_list') and category.level_one_category_list:
        for sub_category in category.level_one_category_list:
            traverse_categories(sub_category, result_list)
    else:
        result_list.append({'id': category.id, 'name': category.name})
        
def obtainDashboardCount(request):
    client_id = get_current_client()
    
    try:
        # Get counts from individual collections
        total_product = products.objects(client_id=ObjectId(client_id)).count()
        total_brand = brand.objects(client_id=ObjectId(client_id)).count()
        
        # Get category stats using the original logic
        last_all_ids = []
        category_list = DatabaseModel.list_documents(category.objects, {'client_id': client_id})
        parent_level_category_list = []
        seen = set()
        
        for category_obj in category_list:
            key = (str(category_obj.id), category_obj.name)
            if key[1] not in seen:
                seen.add(key[1])
                parent_level_category_list.append({'id': key[0], 'name': key[1]})
            traverse_categories(category_obj, last_all_ids)
        
        category_project_dict = {}
        end_level_count_list = []
        
        for i in last_all_ids:
            product_category_config_count = DatabaseModel.count_documents(
                product_category_config.objects, 
                {'category_id': str(i['id'])}
            )
            if product_category_config_count > 0:
                category_project_dict[i['name']] = product_category_config_count
                end_level_count_list.append(str(i['id']))
        
        total_last_level_category = len(set(end_level_count_list))
        total_parent_level_category = len(category_list)
        
        # Get variant stats
        pipeline = [
            {
                '$match': {'client_id': ObjectId(client_id)}
            },
            {
                '$lookup': {
                    'from': 'type_name',
                    'localField': 'option_name_id',
                    'foreignField': '_id',
                    'as': 'type_name'
                }
            },
            {
                '$unwind': {
                    'path': '$type_name',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$lookup': {
                    'from': 'type_value',
                    'localField': 'option_value_id_list',
                    'foreignField': '_id',
                    'as': 'type_value'
                }
            },
            {
                '$unwind': {
                    'path': '$type_value',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$group': {
                    "_id": "$type_name._id",
                    "type_name": {'$first': "$type_name.name"},
                    'option_value_list': {
                        "$push": "$type_value.name",
                    }
                }
            },
            {
                '$project': {
                    "_id": 0,
                    "type_name": 1,
                    'option_value_count': {'$size': '$option_value_list'},
                }
            }
        ]
        
        varent_list = list(varient_option.objects.aggregate(*pipeline))
        
        data = {
            'total_product': total_product,
            'total_brand': total_brand,
            'total_parent_level_category': total_parent_level_category,
            'total_last_level_category': total_last_level_category,
            'category_project_dict': category_project_dict,
            'parent_level_category_list': parent_level_category_list,  # Add this for React
            'varent_list': varent_list
        }
        
        return data
        
    except Exception as e:
        logger.error(f"Error in obtainDashboardCount: {str(e)}")
        return {
            'total_product': 0,
            'total_brand': 0,
            'total_parent_level_category': 0,
            'total_last_level_category': 0,
            'category_project_dict': {},
            'parent_level_category_list': [],  # Add this for React
            'varent_list': []
        }
        
@csrf_exempt
def swapProductToCategory(request):
    json_req = JSONParser().parse(request)
    product_id = json_req.get("product_id")
    category_id = json_req.get("category_id")
    category_name = json_req.get("category_name")
    data = dict()
    product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'product_id':product_id})
    previous_category_id = product_category_config_obj.category_id
    DatabaseModel.update_documents(product_category_config.objects,{'product_id':product_id},{'category_level':category_name,"category_id":category_id})
    category_varient_obj = DatabaseModel.get_document(category_varient.objects,{'category_id':previous_category_id})
    varient_option_id_list = category_varient_obj.varient_option_id_list
    pre_category_varient_obj = DatabaseModel.get_document(category_varient.objects,{'category_id':category_id})
    if pre_category_varient_obj:
        pre_category_varient_obj.category_name = category_name
        pre_category_varient_obj.varient_option_id_list.extend(varient_option_id_list)
        pre_category_varient_obj.save()
    else:
        DatabaseModel.save_documents(category_varient,{'category_id':category_id,'varient_option_id_list':varient_option_id_list,"category_level":category_name})
    data['is_update'] = True
    return data





@csrf_exempt
def createAndAddVarient(request):
    data = dict()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    json_req = JSONParser().parse(request)
    product_id = json_req.get("product_id")
    varient_obj = json_req.get("varient_obj")
    client_id = get_current_client()
    product_obj = DatabaseModel.get_document(products.objects,{"id":product_id})
    sku_number_list = [i.sku_number for i in product_obj.options]
    if varient_obj['sku_number'] not in sku_number_list:
        product_varient_obj = DatabaseModel.save_documents(product_varient,{"sku_number":varient_obj['sku_number'],"finished_price":str(varient_obj['finished_price']),"un_finished_price":str(varient_obj['un_finished_price']),"quantity":varient_obj['quantity'],"retail_price":str(varient_obj['retail_price'])})
        for i in varient_obj['options']:
            product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{"option_name_id":i['option_name_id'],"option_value_id":i['option_value_id']})
            DatabaseModel.update_documents(product_varient.objects,{"id":product_varient_obj.id},{"add_to_set__varient_option_id":product_varient_option_obj.id})
        logForCreateProductVarient(product_varient_obj.id,user_login_id,"Created",{})
        createradial_price_log(product_varient_obj.id,"0",str(varient_obj['retail_price']),user_login_id,client_id)
        DatabaseModel.update_documents(products.objects,{"id":product_id},{"add_to_set__options":product_varient_obj.id})
    else:
        data['status'] = False
        data['error'] = "SKU ALREADY PRESENT"
        return data
    data['status'] = True
    return data


def logForCategory(category_id,action,user_id,level,dict_datas):
    DatabaseModel.save_documents(category_log,{"category_id":str(category_id),"action":str(action),"user_id":ObjectId(user_id),'level':level,'data':dict_datas})
    return 1

def obtainlogForCategoryVarientOption(category_id,category_varient_option_id,action,user_id,category_level,dict_datas):
    DatabaseModel.save_documents(category_varient_option_log,{"category_id":str(category_id),"category_varient_option_id":ObjectId(category_varient_option_id),"user_id":ObjectId(user_id),'action':action,'level':category_level,'data':dict_datas})
    return 1

def logForCreateProduct(product_id,user_id,action,dict_datas):
    if not isinstance(dict_datas,dict) or dict_datas is None:
        dict_datas={}
    DatabaseModel.save_documents(product_log,{"product_id":ObjectId(product_id),"user_id":ObjectId(user_id),'action':action,'data':dict_datas})
    return 1

def logForCreateProductVarient(product_varient_id,user_id,action,dict_datas):
    if not isinstance(dict_datas,dict) or dict_datas is None:
        dict_datas={}
    DatabaseModel.save_documents(product_varient_log,{"product_varient_id":ObjectId(product_varient_id),"user_id":ObjectId(user_id),'action':action,'data':dict_datas})
    return 1



@csrf_exempt
def obtainCategoryLog(request):
    try:
        client_id = get_current_client()
        json_req = json.loads(request.body.decode("utf-8")) if request.body else {}
        action = json_req.get("action", None)
        level = json_req.get("level", None)
        if action:
            filter_obj = {'action':action}
        elif level:
            filter_obj = {'level':level}
        else:
            filter_obj = {}
        pipeline = [
            {
                "$match": filter_obj
            },
            {
            '$lookup': {
                'from': 'user',
                'let': {'user_id': '$user_id'},
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {'$eq': ['$_id', '$$user_id']},
                            'client_id': ObjectId(client_id) 
                        }
                    }
                ],
                'as': 'user_ins'
            }
        },
        {
            '$match': {'user_ins': {'$ne': []}}  
        },
        {
            '$unwind': {
                'path': '$user_ins',
                'preserveNullAndEmptyArrays': False
            }
        },
            {
                '$group': {
                    "_id":None,
                    "category_log_list":{'$push':{"user_name":"$user_ins.name",'category_id':'$category_id','action':'$action','level':'$level','log_date':'$log_date','data':'$data'}}
            }
            }
            ]
        result = list(category_log.objects.aggregate(pipeline))
        data = dict()
        data['result'] = []
        if result:
            for i in result[0]['category_log_list']:
                i['category_last_name'] = ""
                getCategoryLevelOrder(i)
                original_date = i['log_date'] 
                i['log_date_ist'] = convert_to_timezone(original_date, 'Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
                i['log_date'] = convert_to_timezone(original_date, 'US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
            data['result'] = result[0]['category_log_list']
            data['result'] = sorted(data['result'], key=lambda x: x['log_date'],reverse=True)
            return data
        return data
    except Exception as e:
        print(f"Error in obtainCategoryLog: {str(e)}")
        return {'result': [], 'error': str(e)}


@csrf_exempt
def obtainCategoryVarientLog(request):
    client_id = get_current_client()
    json_req = json.loads(request.body.decode("utf-8")) if request.body else {}
    action = json_req.get("action", None)
    level = json_req.get("level", None)
    if action:
        filter_obj = {'action':action}
    elif level:
        filter_obj = {'level':level}
    else:
        filter_obj = {}
    pipeline = [
        {
            "$match": filter_obj
        },
        {
            '$lookup': {
                'from': 'user',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user_ins'
        }
        },
        {
            '$unwind': {
                'path': '$user_ins',
                'preserveNullAndEmptyArrays': True
            }
        },{
            '$match':{'user_ins.client_id':ObjectId(client_id)}
        },
        {
            '$lookup': {
                'from': 'varient_option',
                'localField': 'category_varient_option_id',
                'foreignField': '_id',
                'as': 'varient_option_ins'
        }
        },
        {
            '$unwind': {
                'path': '$varient_option_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            '$lookup': {
                'from': 'type_name',
                'localField': 'varient_option_ins.option_name_id',
                'foreignField': '_id',
                'as': 'type_name_ins'
        }
        },
        {
            '$unwind': {
                'path': '$type_name_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        
        {
            '$group': {
                "_id":None,
                "category_varient_log_list":{'$push':{"user_name":"$user_ins.name",'category_id':'$category_id','action':'$action','level':'$level','varient_option_name':'$type_name_ins.name','log_date':'$log_date','data':'$data'}}
        }
        }
        ]
    result = list(category_varient_option_log.objects.aggregate(*pipeline))
    data = dict()
    data['result'] = []
    if result:
        for i in result[0]['category_varient_log_list']:
            i['category_last_name'] = ""
            getCategoryLevelOrder(i)
            original_date = i['log_date'] 
            i['log_date_ist'] = convert_to_timezone(original_date, 'Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
            i['log_date'] = convert_to_timezone(original_date, 'US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
        data['result'] = result[0]['category_varient_log_list']
        data['result'] = sorted(data['result'], key=lambda x: x['log_date'],reverse=True)

        return data
    return data



@csrf_exempt
def obtainProductLog(request):
    json_req = json.loads(request.body.decode("utf-8")) if request.body else {}
    action = json_req.get("action", None)
    client_id = get_current_client()
    level = json_req.get("level", None)
    if action:
        filter_obj = {'action':action}
    elif level:
        filter_obj = {'level':level}
    else:
        filter_obj = {}
    pipeline = [
        {
            "$match": filter_obj
        },
        {
            '$lookup': {
                'from': 'user',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user_ins'
        }
        },
        {
            '$unwind': {
                'path': '$user_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$lookup': {
                'from': 'products',
                'localField': 'product_id',
                'foreignField': '_id',
                'as': 'products_ins'
        }
        },
        {
            '$unwind': {
                'path': '$products_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            "$match":{'products_ins.client_id':ObjectId(client_id)}
        },
        {
            '$group': {
                "_id":None,
                "category_varient_log_list":{'$push':{"user_name":"$user_ins.name",'product_name':'$products_ins.product_name','action':'$action','log_date':'$log_date','data':'$data'}}
        }
        }
        ]
    result = list(product_log.objects.aggregate(*pipeline))
    data = dict()
    data['result'] = []
    if result:
        for i in result[0]['category_varient_log_list']:
            original_date = i['log_date'] 
            i['log_date_ist'] = convert_to_timezone(original_date, 'Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
            i['log_date'] = convert_to_timezone(original_date, 'US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
        data['result']  = result[0]['category_varient_log_list']
        data['result'] = sorted(data['result'], key=lambda x: x['log_date'],reverse=True)
        
        return data
    return data



def convert_to_timezone(dt, tz_name):
    target_tz = timezone(tz_name)
    if is_naive(dt):
        dt = make_aware(dt)
    return dt.astimezone(target_tz)


@csrf_exempt
def obtainProductVarientLog(request):
    json_req = json.loads(request.body.decode("utf-8")) if request.body else {}
    client_id = get_current_client()
    action = json_req.get("action", None)
    level = json_req.get("level", None)
    if action:
        filter_obj = {'action':action}
    elif level:
        filter_obj = {'level':level}
    else:
        filter_obj = {}
    pipeline = [
        {
            "$match": filter_obj
        },
        {
            '$lookup': {
                'from': 'user',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user_ins'
        }
        },
        {
            '$unwind': {
                'path': '$user_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$lookup': {
                'from': 'product_varient',
                'localField': 'product_varient_id',
                'foreignField': '_id',
                'as': 'product_varient_ins'
        }
        },
        {
            '$unwind': {
                'path': '$product_varient_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$lookup': {
                'from': 'products',
                'localField': 'product_varient_id',
                'foreignField': 'options',
                'as': 'product_ins'
        }
        },
        {
            "$match":{'product_ins.client_id':ObjectId(client_id)}
        },
        {
            '$unwind': {
                'path': '$product_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$group': {
                "_id":None,
                "product_varient_log_list":{'$push':{"user_name":"$user_ins.name",'sku_number':'$product_varient_ins.sku_number','action':'$action','log_date':'$log_date','product_name':'$product_ins.product_name','data':'$data'}}
        }
        }
        ]
    result = list(product_varient_log.objects.aggregate(*pipeline))
    data = dict()
    data['result'] = []
    if result:
        for i in result[0]['product_varient_log_list']:
            original_date = i['log_date'] 
            i['log_date_ist'] = convert_to_timezone(original_date, 'Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
            i['log_date'] = convert_to_timezone(original_date, 'US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
        data['result'] = result[0]['product_varient_log_list']
        data['result'] = sorted(data['result'], key=lambda x: x['log_date'],reverse=True)
        return data
    return data

from django.conf import settings # type: ignore
@csrf_exempt
def createBrand(request):
    name = request.POST.get('name')
    email = request.POST.get('email')
    mobile_number = request.POST.get('mobile_number')
    address = request.POST.get('address')
    website = request.POST.get('website')
    number_of_feeds = request.POST.get('number_of_feeds')
    city = request.POST.get('city')
    state = request.POST.get('state')
    zip = request.POST.get('zip_code')
    # phone_number = request.POST.get('mobile_number')
    image_file = request.FILES.get('logo')
    client_id = get_current_client()
    brand_obj = DatabaseModel.get_document(brand.objects,{'name':name,'client_id':client_id})
    data = dict()
    if brand_obj:
        data['is_created'] = False
        data['error'] = "This Vendor Already Exists"
        return data
    else:
        if image_file:
            # Define the path where the image will be stored
            upload_dir = os.path.join(settings.MEDIA_ROOT, 'brands')  # Ensure this path exists
            os.makedirs(upload_dir, exist_ok=True)

            # Create the full path for the uploaded file
            file_path = os.path.join("media/brands/", image_file.name)
            
            # Save the file to the defined path
            with open(file_path, 'wb+') as destination:
                for chunk in image_file.chunks():
                    destination.write(chunk)  
        else:
            client_obj = DatabaseModel.get_document(client.objects,{'client_id':client_id})
            if client_obj:
                file_path = client_obj.logo
            else:
                file_path=""
        brand_obj = DatabaseModel.save_documents(brand,{'name':name,'logo':file_path,'email':email,'mobile_number':mobile_number,'city': city,'state':state, 'zip_code': zip,'address':address,'website':website,'number_of_feeds':number_of_feeds})
    data['is_created'] = True
    return data

from django.http import JsonResponse

def obtainBrand(request):
    client_id = get_current_client()
    brand_id = request.GET.get('id')
    base_url = f"{request.scheme}://{request.get_host()}/"
    
    # Build match stage
    match_stage = {'client_id': ObjectId(client_id)}
    if brand_id:
        match_stage['_id'] = ObjectId(brand_id)
    
    # Single aggregation with $lookup
    pipeline = [
        {'$match': match_stage},
        {'$sort': {'_id': -1}},
        {
            '$lookup': {
                'from': 'products',  # Your products collection name
                'let': {'brand_id': {'$toString': '$_id'}},
                'pipeline': [
                    {'$match': {'$expr': {'$eq': ['$brand_id', '$$brand_id']}}},
                    {
                        '$group': {
                            '_id': None,
                            'product_count': {'$sum': 1},
                            'sku_count': {'$sum': {'$size': {'$ifNull': ['$options', []]}}}
                        }
                    }
                ],
                'as': 'product_stats'
            }
        },
        {
            '$addFields': {
                'stats': {'$arrayElemAt': ['$product_stats', 0]}
            }
        },
        {
            '$project': {
                '_id': 0,
                'id': {'$toString': '$_id'},
                'brand_number': {'$ifNull': ['$brand_number', '']},
                'name': {'$ifNull': ['$name', '']},
                'email': {'$ifNull': ['$email', '']},
                'mobile_number': {'$ifNull': ['$mobile_number', '']},
                'address': {'$ifNull': ['$address', '']},
                'city': {'$ifNull': ['$city', '']},
                'state': {'$ifNull': ['$state', '']},
                'zip_code': {'$ifNull': ['$zip_code', '']},
                'website': {'$ifNull': ['$website', '']},
                'logo': {
                    '$cond': {
                        'if': {'$and': [{'$ne': ['$logo', None]}, {'$ne': ['$logo', '']}]},
                        'then': {'$concat': [base_url, {'$ltrim': {'input': '$logo', 'chars': '/'}}]},
                        'else': ''
                    }
                },
                'number_of_feeds': '$number_of_feeds',
                'product_count': {'$ifNull': ['$stats.product_count', 0]},
                'sku_count': {'$ifNull': ['$stats.sku_count', 0]}
            }
        }
    ]
    
    result = list(brand.objects.aggregate(pipeline))
    
    return {
        'brand_list': result,
        'brand_count': len(result)
    }
    # pipeline = [
    #     match,
    # {
    #         '$group': {
    #             "_id":'$_id',
    #             # "brand_list":{'$push':{"id":"$_id",'name':'$name','brand_number':'$brand_number','logo':'$logo'}}
    #             'brand_number':{'$first':'$brand_number'},
    #             'name':{'$first':'$name'},
    #             'email':{'$first':'$email'},
    #             'mobile_number':{'$first':'$mobile_number'},
    #             'address':{'$first':'$address'},
    #             'city':{'$first':'$city'},
    #             'state':{'$first':'$state'},
    #             'zip_code':{'$first':'$zip_code'},
    #             'website':{'$first':'$website'},
    #             'number_of_feeds':{'$first':'$number_of_feeds'},
    #             'logo':{'$first':'$logo'},
    #     }
    #     },{
    #         '$project':{
    #             '_id':1,
    #             'brand_number':1,
    #             'name':1,
    #             'email':1,
    #             'mobile_number':1,
    #             'city':1,
    #             'state':1,
    #             'zip_code':1,
    #             'address':1,
    #             'website':1,
    #             'number_of_feeds':1,
    #             'logo':1,
    #         }
    #     },
    # #     {
    # #     '$addFields': {
    # #         'name_lowercase': { '$toLower': "$name" }
    # #     }
    # # },
    # {
    #     '$sort': { '_id': -1 }
    # },
    # ]
    # protocol = 'http'  # You can adjust to 'https' if needed
    
    # # Get the domain (IP or domain name)
    # host = request.get_host()  # Example: 192.168.1.100:8000

    # # Construct the full URL (protocol + host)
    # base_url = f"{protocol}://{host}/"

    # for i in brand_list:
    #     i['id'] = str (i['_id'])
    #     products_list = DatabaseModel.list_documents(products.objects,{'brand_id':i['id']})
    #     i['product_count'] = len(products_list)
    #     i['sku_count'] = 0
    #     for j in products_list:
    #         i['sku_count'] += len(j.options)
    #     del i['_id']
    #     if i['logo']:
    #         i['logo'] = base_url + i['logo'].lstrip('/')
    # data['brand_list'] = brand_list
    # data['brand_count'] = len(data['brand_list'])
    # print('brands',data)
    # return data


import logging
import chardet

logger = logging.getLogger(__name__)
import logging
import chardet
from io import BytesIO, StringIO

logger = logging.getLogger(__name__)

@csrf_exempt
def upload_file(request):
    logger.info("upload_file function called")
    
    data = dict()
    data['status'] = False
    vendor_id = request.POST.get('vendor_id')
    logger.info(f"Vendor ID: {vendor_id}")
    
    if 'file' not in request.FILES:
        logger.warning("No file found in request.FILES")
        data['error'] = "No file uploaded"
        return data
    
    file = request.FILES['file']
    logger.info(f"File received: {file.name}, Size: {file.size} bytes")
    
    try:
        if file.name.endswith('.xlsx'):
            logger.info("Processing XLSX file")
            df = pd.read_excel(file)
        elif file.name.endswith('.csv') or file.name.endswith('.txt'):
            logger.info("Processing CSV/TXT file")
            
            # Read file content
            file.seek(0)
            raw_data = file.read()
            
            # Detect encoding
            detected = chardet.detect(raw_data)
            encoding = detected['encoding']
            confidence = detected['confidence']
            logger.info(f"Detected encoding: {encoding} (confidence: {confidence})")
            
            # List of encodings to try in order
            encodings_to_try = []
            if encoding:
                encodings_to_try.append(encoding)
            encodings_to_try.extend(['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252', 'utf-16', 'utf-8-sig'])
            
            # Remove duplicates while preserving order
            encodings_to_try = list(dict.fromkeys(encodings_to_try))
            
            df = None
            last_error = None
            
            for enc in encodings_to_try:
                try:
                    logger.info(f"Trying encoding: {enc}")
                    decoded_content = raw_data.decode(enc, errors='replace')
                    df = pd.read_csv(StringIO(decoded_content))
                    logger.info(f"Successfully read file with encoding: {enc}")
                    break
                except Exception as e:
                    last_error = str(e)
                    logger.debug(f"Failed with encoding {enc}: {last_error}")
                    continue
            
            if df is None:
                logger.warning("Trying last resort with error handling")
                try:
                    file_like = BytesIO(raw_data)
                    df = pd.read_csv(file_like, encoding='latin-1', encoding_errors='replace', on_bad_lines='skip')
                    logger.info("Successfully read file with latin-1 and error replacement")
                except Exception as e:
                    logger.error(f"All encoding attempts failed. Last error: {last_error}")
                    data['error'] = f"Unable to read file. Please ensure it's a valid CSV/TXT file."
                    return data
        else:
            logger.warning(f"Unsupported file format: {file.name}")
            data['error'] = "Unsupported file format. Please upload .xlsx, .csv, or .txt file"
            return data
            
        logger.info(f"DataFrame loaded successfully. Shape: {df.shape}")
        
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}", exc_info=True)
        data['error'] = f"Error reading file: {str(e)}"
        return data
    
    # Check if DataFrame is empty
    if df is None or df.empty:
        logger.warning("DataFrame is empty")
        data['error'] = "The uploaded file is empty or could not be read"
        return data
    
    try:
        # Clean column names - strip whitespace
        df.columns = df.columns.str.strip()
        
        row_dict = df.iloc[0].to_dict()
        keys_list = list(df.columns)  # Use df.columns instead of row_dict.keys()
        logger.info(f"Total columns extracted: {len(keys_list)}")
        logger.debug(f"Column names: {keys_list}")
    except Exception as e:
        logger.error(f"Error extracting columns: {str(e)}")
        data['error'] = "Error extracting column headers from file"
        return data
    
    # Exclude Option fields and Image fields (commented out in original)
    exclude_fields = [key for key in keys_list if re.match(r"^Option\d+.*", key)]
    logger.info(f"Excluded Option fields: {exclude_fields}")
    
    filtered_keys = [key for key in keys_list if key not in exclude_fields]
    logger.info(f"Filtered keys count: {len(filtered_keys)}")
    
    data['extract_list'] = filtered_keys
    
    user_id = get_current_user()
    logger.info(f"Current user ID: {user_id}")
    
    xl_mapping_obj = DatabaseModel.get_document(
        xl_mapping.objects, 
        {'user_id': user_id, 'vendor_id': vendor_id}
    )
    
    if xl_mapping_obj:
        logger.info(f"XL mapping object found in database with {len(xl_mapping_obj.data)} mappings")
        data['Database_list'] = xl_mapping_obj.data
    else:
        logger.info("No XL mapping object found, using default mapping")
        data['Database_list'] = {
            "MPN": "MPN",
            "model": "model",
            "upc_ean": "upc_ean",
            "product_name": "product_name",
            "long_description": "long_description",
            "short_description": "short_description",
            "Vendor Name": "Vendor Name",
            "Product Category": "Product Category",
            "Type": "Type",
            "msrp": "msrp",
            "base_price": "base_price",
            "Tags": "Tags",
            "Variant_SKU": "Variant_SKU",
            "Un_Finished_Price": "Un_Finished_Price",
            "Finished_Price": "Finished_Price",
            "Key_Features": "Key_Features",
            "stock": "stock",
            "Dimensions": "Dimensions",
            "OPTIONS": "OPTIONS",
            "Image Src": "Image Src"
        }
    
    data['Database_options'] = [
        "MPN", "model", "upc_ean", "product_name", "long_description", 
        "short_description", "Vendor Name", "Product Category", "Type", 
        "msrp", "base_price", "Tags", "Variant_SKU", "Un_Finished_Price", 
        "Finished_Price", "Key_Features", "stock", "Dimensions", "OPTIONS", 
        "Image Src"
    ]
    
    upload_dir = 'uploads/'
    if not os.path.exists(upload_dir):
        logger.info(f"Creating upload directory: {upload_dir}")
        os.makedirs(upload_dir)
    
    try:
        # Save the original file
        file.seek(0)
        fs = FileSystemStorage(location=upload_dir)
        filename = fs.save(file.name, file)
        file_path = os.path.join(fs.location, filename)
        logger.info(f"File saved successfully at: {file_path}")
        
        data['file_path'] = file_path
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}", exc_info=True)
        data['error'] = f"Error saving file: {str(e)}"
        return data
    
    data['vendor_id'] = vendor_id
    data['status'] = True
    
    logger.info(f"upload_file completed successfully. Extracted {len(filtered_keys)} columns")
    return data

import logging
import chardet
from io import StringIO

logger = logging.getLogger(__name__)

@csrf_exempt
def saveXlData(request):
    logger.info("saveXlData function called")
    
    data = dict()
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    field_data = request.POST.get('field_data')
    file_path = request.POST.get('file_path')
    vendor_id = request.POST.get('vendor_id', '')
    
    logger.info(f"User: {user_login_id}, Vendor: {vendor_id}, File: {file_path}")
    
    data['status'] = False
    
    try:
        field_data = json.loads(field_data)
        logger.info(f"Field data parsed successfully with {len(field_data)} mappings")
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing field_data JSON: {str(e)}")
        data['error'] = "Invalid field mapping data"
        return data
    
    # Save/Update XL mapping
    xl_mapping_obj = DatabaseModel.get_document(
        xl_mapping.objects, 
        {'user_id': user_login_id, 'vendor_id': vendor_id}
    )
    
    if xl_mapping_obj:
        logger.info("Updating existing XL mapping")
        DatabaseModel.update_documents(
            xl_mapping.objects,
            {'user_id': user_login_id, 'vendor_id': vendor_id},
            {'data': field_data}
        )
    else:
        logger.info("Creating new XL mapping")
        DatabaseModel.save_documents(
            xl_mapping,
            {'data': field_data, 'user_id': user_login_id, 'vendor_id': ObjectId(vendor_id)}
        )
    
    # Extract field mappings
    model_column_key = field_data.get('model')
    upc_ean_key = field_data.get('upc_ean')
    mpn_key = field_data.get('MPN')
    OPTIONS_key = field_data.get('OPTIONS')
    Vendor_Name_key = field_data.get('Vendor Name')
    product_name_key = field_data.get('product_name')
    long_description_key = field_data.get('long_description')
    short_description_key = field_data.get('short_description')
    brand_key = field_data.get('brand')
    breadcrumb_key = field_data.get('Type')
    Tags_key = field_data.get('Tags')
    Variant_SKU_key = field_data.get('Variant_SKU')
    Un_Finished_Price_key = field_data.get('Un_Finished_Price')
    Finished_Price_key = field_data.get('Finished_Price')
    Key_Features_key = field_data.get('Key_Features')
    category_level_key = field_data.get('Product Category')
    stockv_key = field_data.get('stock')
    dimensions_key = field_data.get('Dimensions')
    images_key = field_data.get('Image Src')
    
    client_id = get_current_client()
    brands_cache={}
    for b in brand.objects.filter(client_id=client_id).only('id','name'):
        brands_cache[b.name.lower()]=b.id 
    type_names_cache={}
    for t in type_name.objects.only('id','name'):
        type_names_cache[t.name.lower()]=t.id  
    type_values_cache={}
    for t in type_value.objects.only('id','name'):
        type_values_cache[t.name.lower()]=t.id 
    categories_cache={
        'level_0':{},
        'level_1':{},
        "level_2":{},
        "level_3":{},
        "level_4":{},
        "level_5":{}
    }
    for c in category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_0'][c.name.lower()]=c.id  
    for c in level_one_category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_1'][c.name.lower()]=c.id  
    for c in level_two_category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_2'][c.name.lower()]=c.id  
    for c in level_three_category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_3'][c.name.lower()]=c.id  
    for c in level_four_category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_4'][c.name.lower()]=c.id  
    for c in level_five_category.objects.filter(client_id=client_id).only('id','name'):
        categories_cache['level_5'][c.name.lower()]=c.id  
    products_cache={}
    for p in products.objects.filter(client_id=client_id).only('id','model','brand_id','options'):
        if p.model:
            products_cache[p.model.strip().lower()]=p.id  
    variants_cache={}
    for v in product_varient.objects.filter(client_id=client_id).only('id','sku_number'):
        variants_cache[v.sku_number]=v.id   
    products_category_cache={}
    for pc in product_category_config.objects.only('product_id','category_id','category_level'):
        products_category_cache[str(pc.product_id.id)]={
            'category_id':pc.category_id,
            'category_level':pc.category_level
        }
    varient_options_cache = {}
    for vo in varient_option.objects.filter(client_id=client_id).only('id','option_name_id','category_str','option_value_id_list'):
        key=f"{vo.option_name_id.id}_{vo.category_str}"
        varient_options_cache[key]={
            'id': vo.id,
            'option_value_ids': [str(v.id) for v in vo.option_value_id_list]
        }
    category_variants_cache={}
    for cv in category_varient.objects.only('id','category_id','varient_option_id_list'):
        category_variants_cache[cv.category_id]={
            "id":cv.id ,
            'variant_option_ids':[str(v.id) for v in cv.varient_option_id_list]
        }
    bulk_logs=[]
    bulk_price_logs=[]
    bulk_product_variant_options=[]
    bulk_variant_options=[]
    # Read and parse file with encoding handling
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            data['error'] = "File not found"
            return data
        
        logger.info(f"Reading file: {file_path}")
        
        if file_path.endswith('.xlsx'):
            logger.info("Processing XLSX file")
            with open(file_path, 'rb') as file:
                df = pd.read_excel(file)
        elif file_path.endswith('.csv') or file_path.endswith('.txt'):
            logger.info("Processing CSV/TXT file with encoding detection")
            
            # Read file content for encoding detection
            with open(file_path, 'rb') as file:
                raw_data = file.read()
            
            # Detect encoding
            detected = chardet.detect(raw_data)
            encoding = detected['encoding']
            confidence = detected['confidence']
            logger.info(f"Detected encoding: {encoding} (confidence: {confidence})")
            
            # List of encodings to try
            encodings_to_try = []
            if encoding:
                encodings_to_try.append(encoding)
            encodings_to_try.extend([
                'utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 
                'windows-1252', 'utf-16', 'utf-8-sig'
            ])
            
            # Remove duplicates
            encodings_to_try = list(dict.fromkeys(encodings_to_try))
            
            df = None
            last_error = None
            
            for enc in encodings_to_try:
                try:
                    logger.info(f"Trying encoding: {enc}")
                    decoded_content = raw_data.decode(enc, errors='replace')
                    df = pd.read_csv(StringIO(decoded_content))
                    logger.info(f"Successfully read file with encoding: {enc}")
                    break
                except Exception as e:
                    last_error = str(e)
                    logger.debug(f"Failed with encoding {enc}: {last_error}")
                    continue
            
            if df is None:
                logger.warning("Trying last resort with latin-1 and error handling")
                try:
                    decoded_content = raw_data.decode('latin-1', errors='replace')
                    df = pd.read_csv(StringIO(decoded_content), on_bad_lines='skip')
                    logger.info("Successfully read file with latin-1 and error replacement")
                except Exception as e:
                    logger.error(f"All encoding attempts failed: {str(e)}")
                    data['error'] = f"Unable to read file. Error: {str(e)}"
                    return data
        else:
            logger.warning(f"Unsupported file format: {file_path}")
            data['error'] = "Unsupported file format"
            return data
        
        # Clean column names
        df.columns = df.columns.str.strip()
        logger.info(f"DataFrame loaded successfully. Shape: {df.shape}")
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        data['error'] = "File not found"
        return data
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}", exc_info=True)
        data['error'] = f"Error reading file: {str(e)}"
        return data
    
    # Check if DataFrame is empty
    if df is None or df.empty:
        logger.warning("DataFrame is empty")
        data['is_error'] = True
        data['error'] = "Excel file should not be empty"
        return data
    
    # Initialize counters
    data['error_list'] = []
    data['is_error'] = False
    data['error_count'] = 0
    data['added_count'] = 0
    data['field_error'] = 0
    data['total_products'] = 0
    optimize_dict = {}
    product_cache={}
    # model_key = str(model).strip() if model else None
    logger.info(f"Starting to process {len(df)} rows")
    
    # Process each row
    for i in range(len(df)):
        dict_error = dict()
        data['total_products'] += 1
        dict_error['is_error'] = False
        dict_error['error_list'] = list()
        
        try:
            row_dict = df.iloc[i].to_dict()
            
            # Extract values with NaN handling
            model = "" if isinstance(row_dict.get(model_column_key), float) and math.isnan(row_dict.get(model_column_key)) else row_dict.get(model_column_key)
            upc_ean = "" if isinstance(row_dict.get(upc_ean_key), float) and math.isnan(row_dict.get(upc_ean_key)) else row_dict.get(upc_ean_key)
            mpn = "" if isinstance(row_dict.get(mpn_key), float) and math.isnan(row_dict.get(mpn_key)) else row_dict.get(mpn_key)
            option_str = "" if isinstance(row_dict.get(OPTIONS_key), float) and math.isnan(row_dict.get(OPTIONS_key)) else row_dict.get(OPTIONS_key)
            brand_name = "" if isinstance(row_dict.get(Vendor_Name_key), float) and math.isnan(row_dict.get(Vendor_Name_key)) else row_dict.get(Vendor_Name_key)
            product_name = "" if isinstance(row_dict.get(product_name_key), float) and math.isnan(row_dict.get(product_name_key)) else row_dict.get(product_name_key)
            long_description = "" if isinstance(row_dict.get(long_description_key), float) and math.isnan(row_dict.get(long_description_key)) else row_dict.get(long_description_key)
            short_description = "" if isinstance(row_dict.get(short_description_key), float) and math.isnan(row_dict.get(short_description_key)) else row_dict.get(short_description_key)
            breadcrumb = None if isinstance(row_dict.get(breadcrumb_key), float) and math.isnan(row_dict.get(breadcrumb_key)) else row_dict.get(breadcrumb_key)
            Tags = None if isinstance(row_dict.get(Tags_key), float) and math.isnan(row_dict.get(Tags_key)) else row_dict.get(Tags_key)
            Variant_SKU = "" if isinstance(row_dict.get(Variant_SKU_key), float) and math.isnan(row_dict.get(Variant_SKU_key)) else row_dict.get(Variant_SKU_key)
            Un_Finished_Price = "0" if isinstance(row_dict.get(Un_Finished_Price_key), float) and math.isnan(row_dict.get(Un_Finished_Price_key)) else row_dict.get(Un_Finished_Price_key)
            Finished_Price = "" if isinstance(row_dict.get(Finished_Price_key), float) and math.isnan(row_dict.get(Finished_Price_key)) else row_dict.get(Finished_Price_key)
            img_src = "" if isinstance(row_dict.get(images_key), float) and math.isnan(row_dict.get(images_key)) else row_dict.get(images_key)
            key_features = "" if isinstance(row_dict.get(Key_Features_key), float) and math.isnan(row_dict.get(Key_Features_key)) else row_dict.get(Key_Features_key)
            stockv = "" if isinstance(row_dict.get(stockv_key), float) and math.isnan(row_dict.get(stockv_key)) else row_dict.get(stockv_key)
            category_level = None if isinstance(row_dict.get(category_level_key), float) and math.isnan(row_dict.get(category_level_key)) else row_dict.get(category_level_key)
            dimensions = "" if isinstance(row_dict.get(dimensions_key), float) and math.isnan(row_dict.get(dimensions_key)) else row_dict.get(dimensions_key)
            model_key = str(model).strip() if model else None

            # Validation
            options = []
            flag = False
            
            if model == "" or model == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Model Name is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if category_level == "" or category_level == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Category Level is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if brand_name == "" or brand_name == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Vendor Name is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if product_name == "" or product_name == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Product Name is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if Variant_SKU == "" or Variant_SKU == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Variant SKU is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if Finished_Price == "" or Finished_Price == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Finished Price is Mandatory")
                dict_error['is_error'] = True
                flag = True
            if long_description == "" or long_description == None:
                dict_error['error-row'] = i + 2
                dict_error['error_list'].append("Long Description is Mandatory")
                dict_error['is_error'] = True
                flag = True
            
            if flag == False:
                data['added_count'] +=1

            # if isinstance(model,str) == False and isinstance(upc_ean,str) == False and isinstance(Variant_SKU,str) == False  and isinstance(category_level,str) == False:
            #     break
            # if isinstance(model,str) == False:
            #     is_varient = True
            # else:
            #     is_varient = False
            # model_key = str(model).strip() if model else None
            model_lookup_key=model_key.lower() if model_key else None
            product_obj=None
            is_varient=False
            if model_lookup_key and model_lookup_key in products_cache:
                product_id=products_cache[model_lookup_key]
                product_obj = DatabaseModel.get_document(products.objects, {'id': product_id})
                if product_obj:
                    is_varient=True
                    logger.info(f"Found existing product in cache: {model} (ID: {product_id})")
            if not product_obj and model_key:
                product_obj = DatabaseModel.get_document(products.objects, {'model': model, 'client_id': ObjectId(client_id)})
            if product_obj:
                product_id = product_obj.id
                products_cache[model_lookup_key] = product_id
                is_varient = True
                logger.info(f"Found existing product in DB: {model} (ID: {product_id})")
            option_name_list = list()
            option_number = 1
            while f'Option{option_number} Name' in row_dict and f'Option{option_number} Value' in row_dict:
                option_name = row_dict[f'Option{option_number} Name']
                option_value = row_dict[f'Option{option_number} Value']
                # if is_varient:
                #     if len(option_name_list) >= option_number:
                #         option_name = option_name_list[option_number - 1]
                #     else:
                #         option_number += 1
                #         continue
                # else:
                #     option_name_list.append(option_name)
                if isinstance(option_name, str) and isinstance(option_value,str):
                    options.append({"name":option_name,"value": option_value})
                option_number += 1
            image_str_list = list()
            option_number = 1
            if img_src:
                img_src = [url.strip() for url in img_src.split(',')]
            else:
                img_src = []
            product_name = product_name.title()
            # if not is_varient:
            #     product_obj = DatabaseModel.get_document(products.objects,{'model':model,'client_id':ObjectId(client_id)})
            if not is_varient:
                if model_lookup_key in products_cache:
                    product_id=products_cache[model_lookup_key]
                    product_obj = DatabaseModel.get_document(products.objects, {'id': product_id})
                else:
                    product_obj=None
                category_list=[]
                if isinstance(category_level,str):
                    category_list = [item.strip() for item in category_level.split('>')]
            if product_obj==None:
                
                category_list = []
                if isinstance(category_level, str):
                    category_list = [item.strip() for item in category_level.split('>')]
                previous_category_id = ""
                for index,i in enumerate(category_list):
                    i = i.title()
                    cat_key=i.lower()
                    if index == 0:
                        if cat_key in categories_cache['level_0']:
                            category_id=categories_cache['level_0'][cat_key]
                        else:    
                            category_obj = DatabaseModel.save_documents(category,{'name':i})
                            category_id=category_obj.id 
                            categories_cache['level_0'][cat_key]=category_id
                            bulk_logs.append({'cat_id':category_id,'action':'Created',"user":user_login_id,'level':'level-1'})
                            logForCategory(category_obj.id,"Created",user_login_id,'level-1',{})
                        previous_category_id = category_id
                    elif index == 1:
                        if cat_key in categories_cache['level_1']:
                            level_one_id=categories_cache['level_1'][cat_key]
                        else:
                            level_one_obj = DatabaseModel.save_documents(level_one_category,{'name':i})
                            level_one_id=level_one_obj.id
                            categories_cache['level_1'][cat_key]=level_one_id
                            bulk_logs.append({'cat_id': level_one_id, 'action': 'Created', 'user': user_login_id, 'level': 'level-1'})
                        DatabaseModel.update_documents(category.objects,{"id":previous_category_id},{"add_to_set__level_one_category_list":level_one_id})
                        previous_category_id = level_one_id
                    elif index == 2:
                        if cat_key in categories_cache['level_2']:
                            level_two_id = categories_cache['level_2'][cat_key]
                        else:
                            level_two_obj = DatabaseModel.save_documents(level_two_category, {'name': i})
                            level_two_id = level_two_obj.id
                            categories_cache['level_2'][cat_key] = level_two_id
                            bulk_logs.append({'cat_id': level_two_id, 'action': 'Created', 'user': user_login_id, 'level': 'level-1'})
                        DatabaseModel.update_documents(level_one_category.objects, {"id": previous_category_id}, {"add_to_set__level_two_category_list": level_two_id})
                        previous_category_id = level_two_id
                    elif index == 3:
                        if cat_key in categories_cache['level_3']:
                            level_three_id = categories_cache['level_3'][cat_key]
                        else:
                            level_three_obj = DatabaseModel.save_documents(level_three_category, {'name': i})
                            level_three_id = level_three_obj.id
                            categories_cache['level_3'][cat_key] = level_three_id
                            bulk_logs.append({'cat_id': level_three_id, 'action': 'Created', 'user': user_login_id, 'level': 'level-1'})
                        DatabaseModel.update_documents(level_two_category.objects, {"id": previous_category_id}, {"add_to_set__level_three_category_list": level_three_id})
                        previous_category_id = level_three_id
                    elif index == 4:
                        if cat_key in categories_cache['level_4']:
                            level_four_id = categories_cache['level_4'][cat_key]
                        else:
                            level_four_obj = DatabaseModel.save_documents(level_four_category, {'name': i})
                            level_four_id = level_four_obj.id
                            categories_cache['level_4'][cat_key] = level_four_id
                            bulk_logs.append({'cat_id': level_four_id, 'action': 'Created', 'user': user_login_id, 'level': 'level-1'})
                        DatabaseModel.update_documents(level_three_category.objects, {"id": previous_category_id}, {"add_to_set__level_four_category_list": level_four_id})
                        previous_category_id = level_four_id
                    elif index == 5:
                        if cat_key in categories_cache['level_5']:
                            level_five_id = categories_cache['level_5'][cat_key]
                        else:
                            level_five_obj = DatabaseModel.save_documents(level_five_category, {'name': i})
                            level_five_id = level_five_obj.id
                            categories_cache['level_5'][cat_key] = level_five_id
                            bulk_logs.append({'cat_id': level_five_id, 'action': 'Created', 'user': user_login_id, 'level': 'level-1'})
                        DatabaseModel.update_documents(level_four_category.objects, {"id": previous_category_id}, {"add_to_set__level_five_category_list": level_five_id})
                        previous_category_id = level_five_id
                brand_key = brand_name.title().lower()
                if brand_key in brands_cache:
                    brand_id=brands_cache[brand_key]
                else:
                    brand_obj = DatabaseModel.save_documents(brand,{'name':brand_name.title(),'client_id':client_id})
                    brand_id = brand_obj.id
                    brands_cache[brand_key] = brand_id
                product_obj = DatabaseModel.save_documents(products,{"model":model,"upc_ean":str(upc_ean),'mpn':str(mpn),"product_name":product_name.title(),"long_description":long_description,"short_description":short_description,"brand_id":brand_id,"breadcrumb":breadcrumb,"key_features":str(key_features),'tags':Tags,'image':img_src,'option_str':option_str,'dimensions':dimensions})
                product_id = product_obj.id
                if model_key:
                    product_cache[model_key]=product_id
                logForCreateProduct(product_id,user_login_id,"Created",{})
                category_level = ""
                if len(category_list) == 1:
                    category_level = "level-1"
                elif len(category_list) == 2:
                    category_level = "level-2"
                elif len(category_list) == 3:
                    category_level = "level-3"
                elif len(category_list) == 4:
                    category_level = "level-4"
                elif len(category_list) == 5:
                    category_level = "level-5"
                elif len(category_list) == 6:
                    category_level = "level-6"
                product_category_config_obj = DatabaseModel.save_documents(product_category_config,{'product_id':product_id,'category_level':category_level,"category_id":str(previous_category_id)})
            else:
                product_id = product_obj.id
                if not is_varient:
                    if brand_name.title() in optimize_dict:
                        brand_id = optimize_dict[brand_name.title()] 
                    else:
                        brand_obj = DatabaseModel.get_document(brand.objects,{'name':brand_name.title(),'client_id':client_id})
                        if brand_obj:
                            brand_id = brand_obj.id
                        else:
                            brand_obj = DatabaseModel.save_documents(brand,{'name':brand_name.title(),'client_id':client_id})
                            brand_id = brand_obj.id
                        optimize_dict[brand_name.title()]  = brand_id
                    update_dict = DatabaseModel.update_documents(products.objects,{'id':product_id},{"model":model,"upc_ean":str(upc_ean),'mpn':str(mpn),"long_description":long_description,"short_description":short_description,"brand_id":brand_id,"breadcrumb":breadcrumb,"key_features":str(key_features),'tags':Tags,'option_str':option_str,'dimensions':dimensions}) 
                    logForCreateProduct(product_id,user_login_id,"Updated",update_dict)
                    if model_key:
                        product_cache[model_key]=product_id
            product_obj.image = img_src
            product_obj.save()
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'product_id':product_id})
            cat_retail_price = 1
            retail_price = "0"
            if Finished_Price == None:
                Finished_Price = "0"
            if Un_Finished_Price == None:
                Un_Finished_Price = "0"
            if product_category_config_obj:
                category_id = product_category_config_obj.category_id
                brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':ObjectId(category_id),'brand_id':product_obj.brand_id.id,'is_active':True})
                if brand_category_price_obj:
                    cat_retail_price = brand_category_price_obj.price
                    if brand_category_price_obj.price_option == 'finished_price':
                        retail_price = str(float(Finished_Price) * float(cat_retail_price))
                    else:
                        retail_price = str(float(Un_Finished_Price) * float(cat_retail_price))
                else:
                    retail_price = str(float(Finished_Price) * float(1))
            # sku_number_list = [i.sku_number for i in product_obj.options]
            # sku_number_id_list = [i.id for i in product_obj.options]
            if Variant_SKU not in variants_cache:
                product_varient_obj = DatabaseModel.save_documents(product_varient,{"sku_number":Variant_SKU,"finished_price":str(Finished_Price),"un_finished_price":str(Un_Finished_Price),"quantity":str(stockv),"retail_price":retail_price})
                variants_cache[Variant_SKU] = product_varient_obj.id
                createradial_price_log(product_varient_obj.id,"0",retail_price,user_login_id,client_id)
                logForCreateProductVarient(product_varient_obj.id,user_login_id,"Created",{})
                for i in options:
                    type_name_key=i['name'].title().lower()
                    if type_name_key in type_names_cache:
                        type_name_id=type_names_cache[type_name_key]

                    else:
                        type_name_obj = DatabaseModel.get_document(type_name.objects,{'name':i['name'].title()})
                        if type_name_obj==None:
                            type_name_obj = DatabaseModel.save_documents(type_name,{'name':i['name'].title()})
                        type_name_id=type_name_obj.id 
                        type_names_cache[type_name_key]=type_name_id
                    type_value_key=str(i['value']).title().lower()
                    if type_value_key in type_values_cache:
                        type_value_id=type_values_cache[type_value_key]
                    else:
                        type_value_obj = DatabaseModel.save_documents(type_value, {'name': str(i['value']).title()})
                        type_value_id = type_value_obj.id
                        type_values_cache[type_value_key] = type_value_id
                        
                    # if i['name'].title() in optimize_dict:
                    #     type_name_id = optimize_dict[i['name'].title()] 
                    # else:
                    #     type_name_obj = DatabaseModel.get_document(type_name.objects,{'name':i['name'].title()})
                    #     if type_name_obj ==None:
                    #         type_name_obj = DatabaseModel.save_documents(type_name,{'name':i['name'].title()})   
                    #     type_name_id = type_name_obj.id
                    #     optimize_dict[i['name'].title()] = type_name_id
                    # if str(i['value']).title() in optimize_dict:
                    #     type_value_id = optimize_dict[str(i['value']).title()] 
                    # else:
                    #     type_value_obj = DatabaseModel.get_document(type_value.objects,{'name':str(i['value']).title()})
                    #     if type_value_obj ==None:
                    #         type_value_obj = DatabaseModel.save_documents(type_value,{'name':str(i['value']).title()})   
                    #     type_value_id = type_value_obj.id
                    #     optimize_dict[str(i['value']).title()] = type_value_id
                    product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{"option_name_id":type_name_id,"option_value_id":type_value_id})
                    DatabaseModel.update_documents(product_varient.objects,{"id":product_varient_obj.id},{"add_to_set__varient_option_id":product_varient_option_obj.id})
                    DatabaseModel.update_documents(products.objects,{"id":product_id},{"add_to_set__options":product_varient_obj.id,'add_to_set__image':image_str_list})
                    vo_cache_key = f"{type_name_id}_{category_id}"
                    if vo_cache_key in varient_options_cache:
                        varient_option_obj_id = varient_options_cache[vo_cache_key]['id']
                        if str(type_value_id) not in varient_options_cache[vo_cache_key]['option_value_ids']:
                            DatabaseModel.update_documents(varient_option.objects, {"id": varient_option_obj_id}, {"add_to_set__option_value_id_list": type_value_id})
                            varient_options_cache[vo_cache_key]['option_value_ids'].append(str(type_value_id))
                        varient_option_obj = DatabaseModel.get_document(varient_option.objects, {"id": varient_option_obj_id})
                    else:
                        varient_option_obj = DatabaseModel.save_documents(varient_option, {"option_name_id": type_name_id, 'category_str': str(category_id), 'client_id': client_id, "option_value_id_list": [type_value_id]})
                        varient_options_cache[vo_cache_key] = {'id': varient_option_obj.id,'option_value_ids': [str(type_value_id)]}
                    

                    category_varient_obj = DatabaseModel.get_document(category_varient.objects,{'category_id':str(category_id)})
                    if category_varient_obj == None:
                        obtainlogForCategoryVarientOption(category_id,varient_option_obj.id,"create",ObjectId(user_login_id),category_level,{})
                        DatabaseModel.save_documents(category_varient,{'category_id':category_id,'varient_option_id_list':[varient_option_obj.id]})
                    else:
                        obtainlogForCategoryVarientOption(category_id,varient_option_obj.id,"create",ObjectId(user_login_id),category_level,{})
                        DatabaseModel.update_documents(category_varient.objects,{"id":category_varient_obj.id},{'category_id':category_id,'add_to_set__varient_option_id_list':varient_option_obj.id})
    
            else:
                updated_fields = DatabaseModel.update_documents(product_varient.objects,{"sku_number":Variant_SKU,'client_id':client_id},{"finished_price":str(Finished_Price),"un_finished_price":str(Un_Finished_Price),"quantity":str(stockv),"retail_price":retail_price})
                product_varient_obj = DatabaseModel.get_document(product_varient.objects,{"sku_number":Variant_SKU,'client_id':client_id})
                if product_varient_obj:
                    createradial_price_log(product_varient_obj.id,"0",retail_price,user_login_id,client_id)
                    dict_datas = {}
                    logForCreateProductVarient(product_varient_obj.id,user_login_id,"Updated",updated_fields)
                    for i in options:
                        if i['name'].title() in optimize_dict:
                            type_name_id = optimize_dict[i['name'].title()] 
                        else:
                            type_name_obj = DatabaseModel.get_document(type_name.objects,{'name':i['name'].title()})
                            if type_name_obj ==None:
                                type_name_obj = DatabaseModel.save_documents(type_name,{'name':i['name'].title()})   
                            type_name_id = type_name_obj.id
                            optimize_dict[i['name'].title()] = type_name_id
                        if str(i['value']).title() in optimize_dict:
                            type_value_id = optimize_dict[str(i['value']).title()] 
                        else:
                            type_value_obj = DatabaseModel.get_document(type_value.objects,{'name':str(i['value']).title()})
                            if type_value_obj ==None:
                                type_value_obj = DatabaseModel.save_documents(type_value,{'name':str(i['value']).title()})   
                            type_value_id = type_value_obj.id
                            optimize_dict[str(i['value']).title()] = type_value_id
                        product_varient_option_check = DatabaseModel.get_document(product_varient_option.objects,{"option_name_id":type_name_id,"option_value_id":type_value_id})
                        if product_varient_option_check == None:
                            product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{"option_name_id":type_name_id,"option_value_id":type_value_id})
                            DatabaseModel.update_documents(product_varient.objects,{"id":product_varient_obj.id},{"add_to_set__varient_option_id":product_varient_option_obj.id})
                            DatabaseModel.update_documents(products.objects,{"id":product_id},{"add_to_set__options":product_varient_obj.id,
                            'add_to_set__image':image_str_list})
                        varient_option_obj = DatabaseModel.get_document(varient_option.objects,{"option_name_id":type_name_id,'category_str':str(category_id),'client_id':client_id})
                        if varient_option_obj:
                            DatabaseModel.update_documents(varient_option.objects,{"option_name_id":type_name_id,'category_str':str(category_id),'client_id':client_id},{"add_to_set__option_value_id_list":type_value_id})
                        else:
                            varient_option_obj = DatabaseModel.save_documents(varient_option,{"option_name_id":type_name_id,'category_str':str(category_id),'client_id':client_id,"option_value_id_list":[type_value_id]})
                        
                        category_varient_obj = DatabaseModel.get_document(category_varient.objects,{'category_id':str(category_id)})
                        if category_varient_obj == None:
                            obtainlogForCategoryVarientOption(category_id,varient_option_obj.id,"Updated",ObjectId(user_login_id),category_level,{})
                            DatabaseModel.save_documents(category_varient,{'category_id':category_id,'varient_option_id_list':[varient_option_obj.id]})
                        else:
                            obtainlogForCategoryVarientOption(category_id,varient_option_obj.id,"Updated",ObjectId(user_login_id),category_level,{})
                            DatabaseModel.update_documents(category_varient.objects,{"id":category_varient_obj.id},{'category_id':category_id,'add_to_set__varient_option_id_list':varient_option_obj.id})

                
        except Exception as e:
            logger.error(f"Error processing row {i + 2}: {str(e)}", exc_info=True)
            dict_error['error-row'] = i + 2
            dict_error['error_list'].append(f"Error processing row: {str(e)}")
            dict_error['is_error'] = True
        
        if len(dict_error['error_list']) > 0:
            data['error_list'].append(dict_error)
            if dict_error['is_error']:
                data['is_error'] = True
                data['error_count'] += 1
    
    # Clean up uploaded file
    upload_dir = os.path.dirname(file_path)
    try:
        if os.path.exists(upload_dir) and os.path.isdir(upload_dir):
            shutil.rmtree(upload_dir)
            logger.info(f"Folder {upload_dir} has been deleted successfully")
        else:
            logger.warning(f"Upload directory {upload_dir} does not exist")
    except Exception as e:
        logger.error(f"Error deleting upload folder: {str(e)}")
    logger.info("Processing bulk logs...")
    if bulk_logs:
        for log_item in bulk_logs:
            logForCategory(log_item['cat_id'], log_item['action'], log_item['user'], log_item['level'], {})
    logger.info(f"Processed {len(bulk_logs)} category logs")
    data['status'] = True
    
    
    return data

def obtainPriceLog(request):
    try:
        client_id = get_current_client()
        pipeline = [
    {'$match': {'client_id': ObjectId(client_id)}},
    {'$sort': {'log_date': -1}},
    {
        '$lookup': {
            'from': 'product_varient',
            'localField': 'product_varient_id',
            'foreignField': '_id',
            'as': 'product_info'
        }
    },
    {
        '$lookup': {
            'from': 'user',
            'localField': 'user_id',
            'foreignField': '_id',
            'as': 'user_info'
        }
    },
    {
        '$project': {
            '_id': 0,
            'sku_number': {
                '$ifNull': [
                    {'$arrayElemAt': ['$product_info.sku_number', 0]},
                    'Unknown'
                ]
            },
            'old_retail_price': 1,
            'new_retail_price': 1,
            'user_id': {
                '$ifNull': [
                    {'$arrayElemAt': ['$user_info.name', 0]},
                    'Unknown User'
                ]
            },
            'log_date': 1
        }
    }
]
        result_list = list(radial_price_log.objects.aggregate(pipeline))
        
        return result_list
    except Exception as e:
        print(f"Error in obtainPriceLog: {str(e)}")
# import requests
# from bs4 import BeautifulSoup
from django.http import JsonResponse # type: ignore

# @csrf_exempt
# def sampleData(req):
#     data_list = data_fn()
#     for data in data_list:
#         category_name = "level-3"
#         category_id = "6728c594f254bfbc4a59a6b6"
#         product_obj = data['product_obj']
#         product_obj_save = {
#             "model" :product_obj['model'],
#             "upc_ean" :product_obj['upc_ean'],
#             "mpn" :product_obj['mpn'],     
#             "breadcrumb":product_obj['breadcrumb'],
#             "brand_id":ObjectId(product_obj['brand_id']),
#             "product_name":product_obj['product_name'],
#             "long_description":product_obj['long_description'],
#             "short_description":product_obj['short_description'],
#             "features":product_obj['features'],
#             "attributes":product_obj['attributes'],
#             "tags":product_obj['tags'],
#             "msrp":str(product_obj['msrp']),
#             "base_price":str(product_obj['base_price']),
#             "key_features":product_obj['key_features'],
#             "image":product_obj['image_str_list']
#         }
#         products_obj_1 = DatabaseModel.save_documents(products,product_obj_save)
#         user_login_id = "6729ac25db9f51647ea76af4"
#         for z in product_obj['varients']:
#             product_varient_obj = DatabaseModel.save_documents(product_varient,{"sku_number":z['sku_number'],"finished_price":str(z['finished_price']),"un_finished_price":str(z['un_finished_price']),"quantity":z['quantity']})
#             logForCreateProductVarient(product_varient_obj.id,user_login_id,"create")
#             for i in z['options']:
#                 type_name_obj = DatabaseModel.get_document(type_name.objects,{'name':i['name'].title()})
#                 if type_name_obj ==None:
#                     type_name_obj = DatabaseModel.save_documents(type_name,{'name':i['name'].title()})   
#                 type_name_id = type_name_obj.id
#                 type_value_obj = DatabaseModel.get_document(type_value.objects,{'name':i['value'].title()})
#                 if type_value_obj ==None:
#                     type_value_obj = DatabaseModel.save_documents(type_value,{'name':i['value'].title()})   
#                 type_value_id = type_value_obj.id
#                 product_varient_option_obj = DatabaseModel.save_documents(product_varient_option,{"option_name_id":type_name_id,"option_value_id":type_value_id})
#                 DatabaseModel.update_documents(product_varient.objects,{"id":product_varient_obj.id},{"add_to_set__varient_option_id":product_varient_option_obj.id})
#             DatabaseModel.update_documents(products.objects,{"id":products_obj_1.id},{"add_to_set__options":product_varient_obj.id})
#         products_obj = DatabaseModel.save_documents(product_category_config,{'product_id':products_obj_1.id,'category_level':category_name,"category_id":category_id})
#         logForCreateProduct(products_obj_1.id,user_login_id,"create")
#     data = dict()
#     data['status'] = True
#     return JsonResponse(data)




from .data_py import data_list_file
def data_fn():
    return data_list_file



#SuperAdmin
@csrf_exempt
def createClient(request):
    json_req = JSONParser().parse(request)
    name = json_req.get("name").title()
    logo = json_req.get("logo")
    location = json_req.get("location")
    client_obj = DatabaseModel.get_document(client.objects,{'name':name})
    data = dict()
    if client_obj:
        data['is_created'] = False
        data['error'] = "client Already Present"
        return data
    else:
        client_obj = DatabaseModel.save_documents(client,{'name':name,'logo':logo,'location':location})
    data['is_created'] = True
    return data


@csrf_exempt
def obtainClient(request):
    data = dict()
    pipeline = [
    {
            '$group': {
                "_id":None,
                "client_list":{'$push':{"id":"$_id",'name':'$name','logo':'$logo','location':'$location'}}
        }
        }
    ]
    client_list = list(client.objects.aggregate(*pipeline))
    data['client_list'] = list() 
    if client_list:
        for client_ins in client_list[0]['client_list']:
            client_ins['id'] = str(client_ins['id'])
        data['client_list'] = client_list[0]['client_list']
    return data


def obtainSuperAdminDashboard(request):
    data = dict()
    data['clients_count'] = DatabaseModel.count_documents(client.objects,{})
    data['active_users_count'] = 0
    return data

@csrf_exempt
def obtainClientDetail(request):
    client_id = request.GET.get("id")
    data = dict()
    pipeline = [
        {
            '$match':{'_id':ObjectId(client_id)}
        }, {
            '$lookup': {
                'from': 'user',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user_ins'
        }
        },
        {
            '$unwind': {
                'path': '$user_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
            '$group': {
                "_id":None,
                "id":{'$push':"$_id"},
                'name':{'$push':'$name'},
                'logo':{'$push':'$logo'},
                'location':{'$push':'$location'},
                "user_list":{'$push':{'name':'$user_ins.name','role':'$user_ins.role'}}
        }
        }
    ]
    client_list = list(client.objects.aggregate(*pipeline))
    data['client_obj'] = dict()
    if client_list:
        client_list = client_list[0]
        client_list['id'] = str(client_list['id'])
        data['client_obj'] = client_list
    return data

@csrf_exempt
def addUserAndRoleForClient(request):
    json_req = JSONParser().parse(request)
    user_obj = json_req.get("user_obj")
    id = json_req.get("id")
    data = dict()
    user_obj = DatabaseModel.get_document(user.objects,{'name':'','email':""})
    if user_obj:
        data['is_created'] = False
        data['error'] = "user Already Present"
        return data
    else:
        DatabaseModel.save_documents(user,{'name':user_obj['name'],'email':user_obj['email'],'role':user_obj['role'],'client_id':ObjectId(user_obj['id'])})

@csrf_exempt
def categoryLevelForChildCategory(request):
    json_req = JSONParser().parse(request)
    dist_i = dict()
    dist_i['level'] = json_req['category_level']
    dist_i['category_id'] = json_req['category_id']
    dist_i['category_name'] = ""
    getCategoryLevelOrder(dist_i)
    return dist_i

def obtainClientName(request):
    client_id = get_current_client()
    client_obj = DatabaseModel.get_document(client.objects,{'id':client_id})
    data = dict()
    data['logo'] = ""
    data['name'] = ""
    if client_obj:
        data['name'] = client_obj.name
        data['logo'] = client_obj.logo
    return data

# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from odf.opendocument import load
# from odf.table import Table, TableRow, TableCell
# import os

# @csrf_exempt
# def sampleData(request):
#     data = {'status': False}
#     if 'file' not in request.FILES:
#         return JsonResponse(data, status=400)  # Return 400 Bad Request if no file is uploaded
#     file = request.FILES['file']
#     file_path = os.path.join('/tmp', file.name)
#     try:
#         with open(file_path, 'wb') as f:
#             for chunk in file.chunks():
#                 f.write(chunk)
#         # Convert ODS to JSON
#         json_data = ods_to_json(file_path)
#         data['status'] = True
#         data['data'] = json_data
#         return JsonResponse(data, safe=False)
#     finally:
#         if os.path.exists(file_path):
#             os.remove(file_path)  # Clean up the temporary file even if an error occurs

# def ods_to_json(file_path):
#     doc = load(file_path)
#     data = []
#     for sheet in doc.getElementsByType(Table):
#         sheet_data = []
#         for row in sheet.getElementsByType(TableRow):
#             row_data = {}
#             cells = row.getElementsByType(TableCell)
#             if len(cells) > 1:
#                 key = ''.join(node.nodeValue for node in cells[0].childNodes if node.nodeType == 3).strip() 
#                 value = ''.join(node.nodeValue for node in cells[1].childNodes if node.nodeType == 3).strip()
#                 if key and value:
#                     row_data[key] = value
#             if row_data:
#                 sheet_data.append(row_data)
#         if sheet_data:
#             data.append(sheet_data)
#     return data

def  obtainAllLastLevelIds(request):
    client_id = get_current_client()
    last_all_ids = []
    category_list = DatabaseModel.list_documents(category.objects,{'client_id':ObjectId(client_id)})
    for category_obj in category_list:
        if len(category_obj.level_one_category_list)>0:
            for i in category_obj.level_one_category_list:
                if len(i.level_two_category_list)>0:
                    for j in i.level_two_category_list:
                        if len(j.level_three_category_list)>0:
                            for k in j.level_three_category_list:
                                if len(k.level_four_category_list)>0:
                                    for l in  k.level_four_category_list:
                                        if len(l.level_five_category_list)>0:
                                            for m in  l.level_five_category_list:
                                                last_all_ids.append({'id':str(m.id),'name':m.name})
                                        else:
                                            last_all_ids.append({'id':str(l.id),'name':l.name})
                                else:
                                    last_all_ids.append({'id':str(k.id),'name':k.name})
                        else:
                            last_all_ids.append({'id':str(j.id),'name':j.name})
                else:
                    last_all_ids.append({'id':str(i.id),'name':i.name})
        else:
            last_all_ids.append({'id':str(category_obj.id),'name':category_obj.name})
    data = dict()
    data['last_level_category'] = last_all_ids
    return data

@csrf_exempt
def obtainBrandCategoryWisePrice(request):
    json_req = JSONParser().parse(request)
    brand_category_price_obj_list = DatabaseModel.list_documents(brand_category_price.objects,{'category_id__in':json_req['category_id_list'],'brand_id':ObjectId(json_req['brand_id'])})
    data = dict()
    data['price_list'] = []
    for i in  brand_category_price_obj_list:
        data['price_list'].append(i.price)
    return data


def createBrandCategoryWisePrice(json_req):
    brand_category_price_obj_1 = DatabaseModel.list_documents(brand_category_price.objects,{'category_id__in':json_req['category_id_list'],'brand_id':ObjectId(json_req['brand_id'])})
    for i in brand_category_price_obj_1:
        brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'is_active':True})
        if brand_category_price_obj:
            DatabaseModel.update_documents(brand_category_price.objects,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'is_active':True},{'is_active':False})
        brand_category_price_obj_ch = DatabaseModel.get_document(brand_category_price.objects,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'price':json_req['price'],'price_option':json_req['price_option']})
        if brand_category_price_obj_ch:
            DatabaseModel.update_documents(brand_category_price.objects,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'price':json_req['price'],'price_option':json_req['price_option']},{'is_active':True})
        else:
            DatabaseModel.save_documents(brand_category_price,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'is_active':True,'price':str(json_req['price']),'price_option':json_req['price_option']})
    if len(brand_category_price_obj_1) == 0:
        for i in json_req['category_id_list']:
            DatabaseModel.save_documents(brand_category_price,{'category_id':str(i),'brand_id':ObjectId(json_req['brand_id']),'is_active':True,'price':json_req['price'],'price_option':json_req['price_option']})
    data = dict()
    data['is_created'] = True
    return data

from bson.dbref import DBRef # type: ignore
@csrf_exempt
def updateRetailPrice(request):
    json_req = JSONParser().parse(request)
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    product_category_config_list = DatabaseModel.list_documents(product_category_config.objects,{'category_id__in':json_req['category_id_list']})
    createBrandCategoryWisePrice(json_req)
    old_price = '0'
    
    for i in product_category_config_list:
        for j in i.product_id.options:
            if str(i.product_id.brand_id.id) == json_req['brand_id']:
                brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':str(i.category_id),'brand_id':ObjectId(json_req['brand_id']),'is_active':True})
                if isinstance(j, DBRef):
                    old_price = '0'
                    j.retail_price = json_req['price']
                else:
                    old_price = j.retail_price if 'retail_price' in j else '0'
                if brand_category_price_obj:
                    if brand_category_price_obj.price_option == "finished_price":
                        
                        if j.finished_price == '' or j.finished_price =='None'or j.finished_price ==None:
                            j.finished_price = '0' 
                        j.retail_price = str(float(j.finished_price) * float(json_req['price']))
                    else:
                        
                        if j.un_finished_price == '' or j.un_finished_price =='None' or j.un_finished_price ==None:
                            j.un_finished_price = '0'
                        j.retail_price = str(float(j.un_finished_price) * float(json_req['price']))
                if j.retail_price == None:
                    
                    j.retail_price = "0"
            j.save()
            client_id = get_current_client()
            
            createradial_price_log(j.id,str(old_price),str(j.retail_price),user_login_id,client_id)
    data = dict()
    data['is_updated'] = True
    return data


@csrf_exempt
def obtainBrandCategoryWisePriceTable(request):
    json_req = JSONParser().parse(request)
    if json_req.get('category_id_list') == None:
        brand_category_price_obj_list = DatabaseModel.list_documents(brand_category_price.objects,{'brand_id':ObjectId(json_req['brand_id'])})
    else:
        if json_req['category_id_list'] == []:
            brand_category_price_obj_list = DatabaseModel.list_documents(brand_category_price.objects,{'brand_id':ObjectId(json_req['brand_id'])})
        else:
            brand_category_price_obj_list = DatabaseModel.list_documents(brand_category_price.objects,{'category_id__in':json_req['category_id_list'],'brand_id':ObjectId(json_req['brand_id'])})
    data = dict()
    data['category_list'] = list()
    for brand_category_price_obj in  brand_category_price_obj_list:
        category_obj = DatabaseModel.get_document(category.objects,{'id':brand_category_price_obj.category_id})
        if category_obj == None:
            category_obj = DatabaseModel.get_document(level_one_category.objects,{'id':brand_category_price_obj.category_id})
        if category_obj == None:
            category_obj = DatabaseModel.get_document(level_two_category.objects,{'id':brand_category_price_obj.category_id})
        if category_obj == None:
            category_obj = DatabaseModel.get_document(level_three_category.objects,{'id':brand_category_price_obj.category_id})
        if category_obj == None:
            category_obj = DatabaseModel.get_document(level_four_category.objects,{'id':brand_category_price_obj.category_id})
        if category_obj == None:
            category_obj = DatabaseModel.get_document(level_five_category.objects,{'id':brand_category_price_obj.category_id})
        data['category_list'].append({"category_name":str(category_obj.name),"id":str(brand_category_price_obj.category_id),"price":brand_category_price_obj.price,"brand_name":brand_category_price_obj.brand_id.name,"brand_id":str(brand_category_price_obj.brand_id.id),"is_active":brand_category_price_obj.is_active,'brand_category_price_id':str(brand_category_price_obj.id),'price_option':brand_category_price_obj.price_option})
    data['category_list'] = sorted(data['category_list'], key=lambda x: ObjectId(x['brand_category_price_id']),reverse=True)

    return data


def createradial_price_log(product_varient_id,old_retail_price,new_retail_price,user_login_id,client_id):
    DatabaseModel.save_documents(radial_price_log,{"product_varient_id":product_varient_id,'old_retail_price':old_retail_price,'new_retail_price':new_retail_price,'user_id':ObjectId(user_login_id),'client_id':ObjectId(client_id)})
    return 1

@csrf_exempt
def updateActiveRetailPrice(request):
    json_req = JSONParser().parse(request)
    brand_category_price_obj_1 = DatabaseModel.update_documents(brand_category_price.objects,{'category_id':json_req['category_id'],'brand_id':json_req['brand_id'],'is_active':True},{'is_active':False})
    brand_category_price_obj_2 = DatabaseModel.update_documents(brand_category_price.objects,{'category_id':json_req['category_id'],'brand_id':json_req['brand_id'],'price':str(json_req['price'])},{'is_active':True})
    data = dict()
    data['is_updated'] = True
    return data
    
@csrf_exempt
def obtainRetailBrandPrice(request):
    json_req = JSONParser().parse(request)
    brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'is_active':True})
    data = dict()
    if brand_category_price_obj:
        data['price'] = brand_category_price_obj.price
        data['price_option'] = brand_category_price_obj.price_option
    else:
        data['price'] = 1
        data['price_option'] = "finished_price"
    return data
from django.core.mail import send_mail

@csrf_exempt
def createUser(request):
    client_id = get_current_client()
    json_req = JSONParser().parse(request)
    user_obj = DatabaseModel.get_document(user,{'client_id':ObjectId(client_id),'user_name':json_req['user_name']})
    if user_obj:
        data = dict()
        data['is_created'] = False
        return data
    user_obj = DatabaseModel.save_documents(user,{'client_id':ObjectId(client_id),'user_name':json_req['user_name'],'name':json_req['name'],'email':json_req['email'],'role':json_req['role'],'password':json_req['password']})
    data = dict()
    send_mail(
        'Your account has been created successfully',
        f"Your password {user_obj['password']}",
        settings.EMAIL_HOST_USER,
        [json_req['email']],
        fail_silently=False,
    )
    data['is_created'] = True
    return data


@csrf_exempt
def obtainVarientOptionForRetailPrice(request):
    data = dict()
    data['varient_option_list'] = []
    client_id = get_current_client()
    varient_option_list = DatabaseModel.list_documents(varient_option.objects, {'client_id': ObjectId(client_id)})
    unique_ids = set()
    for i in varient_option_list:
        try:
            option_id = str(i.option_name_id.id)
        except:
            continue
            
            
        # if option_id not in unique_ids:
        unique_ids.add(option_id)
        collections = [
            category,
            level_five_category,
            level_four_category,
            level_three_category,
            level_two_category,
            level_one_category
        ]

        category_name = None

        for collection in collections:
            try:
                # Use this line if you're using MongoEngine
                category_obj = collection.objects(id=i.category_str).first()

                # Use this line instead if you're using PyMongo
                # category_obj = collection.find_one({'id': i['category_id']})

                if category_obj:
                    category_name = category_obj.name
                    break
            except Exception as e:
                print(f"Error checking collection {collection}: {e}")

        
        if category_name:
            i.option_name_id.name = i.option_name_id.name +" " +"("+ category_name+")"
        data['varient_option_list'].append({'id': str(i.id), "name": i.option_name_id.name})
    return data


@csrf_exempt
def obtainVarientOptionValueForRetailPrice(request):
    data = dict()
    option_name_id = request.GET.get('id')
    client_id = get_current_client()
    data['varient_option_value_list'] = []
    varient_option_obj = DatabaseModel.get_document(varient_option.objects,{'id':option_name_id,'client_id':client_id})
    unique_ids = set()

    if varient_option_obj:
        for i in varient_option_obj.option_value_id_list:
            option_id = str(i.id)
            if option_id not in unique_ids:
                data['varient_option_value_list'].append({'id':str(i.id),"name":i.name})
                unique_ids.add(option_id)

    return data


# @csrf_exempt
# def updateRetailPriceForVarientOptionValue(request):
#     json_req = JSONParser().parse(request)
#     user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
#     percentage_price = json_req['percentage_price']
#     products_list = DatabaseModel.list_documents(products.objects,{'brand':json_req['brand_id']})
#     for i in products_list:
#         for j in i.options:
#             if json_req['option_name_id'] ==  str(j.varient_option_id.option_name_id.id)  and j.varient_option_id.option_value_id.id in json_req['option_value_id_list']:
#                 if json_req['price_option'] == 'finished_price':
#                     j.retail_price = ((j.finished_price/100)*percentage_price) + j.finished_price
#                 else:
#                     j.retail_price = ((j.un_finished_price/100)*percentage_price) + j.un_finished_price
#     data = dict()
#     data['is_updated'] = True
#     return data
@csrf_exempt
def obtainProductBasedOnVarientOption(request):
    json_req = JSONParser().parse(request)
    price_option = json_req['price_option'] 
    option_name_id = ""
    varient_option_obj = DatabaseModel.get_document(varient_option.objects,{'id':json_req['option_name_id']})
    if varient_option_obj:
        option_name_id = varient_option_obj.option_name_id.id
    option_value_id_list = [ObjectId(i) for i in json_req['option_value_id']]
    pipeline = [
    {
            "$match":{'brand_id':ObjectId(json_req['brand_id'])}
        },
        {
            '$lookup': {
                "from": 'product_varient',
                "localField": 'options',
                "foreignField": "_id",
                "as": "product_varient_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_varient_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            '$lookup': {
                "from": 'product_varient_option',
                "localField": 'product_varient_ins.varient_option_id',
                "foreignField": "_id",
                "as": "product_varient_option_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_varient_option_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            "$match":{'product_varient_option_ins.option_name_id':option_name_id,'product_varient_option_ins.option_value_id':{'$in':(option_value_id_list)}}
        },
        {
        '$lookup': {
            'from': 'type_name',
            'localField': 'product_varient_option_ins.option_name_id',
            'foreignField': '_id',
            'as': 'type_name'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_name',
                'preserveNullAndEmptyArrays': True
            }
        },    {
        '$lookup': {
            'from': 'type_value',
            'localField': 'product_varient_option_ins.option_value_id',
            'foreignField': '_id',
            'as': 'type_value'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_value',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
        '$group': {
            "_id":"$product_varient_ins._id",
            "product_id": { "$first": "$_id" },
            "product_varient_id": { "$first": "$product_varient_ins._id" },
            "product_name": { "$first": "$product_name" },
            "sku_number": { "$first": "$product_varient_ins.sku_number" },
            "finished_price": { "$first": "$product_varient_ins.finished_price" },
            "un_finished_price": { "$first": "$product_varient_ins.un_finished_price" },
            "retail_price": { "$first": "$product_varient_ins.retail_price" },
            "dimensions": { "$first": "$dimensions" },
            
            "image_url": { "$first": "$image" },
            'varient_option_list': {
                "$push": {
                    'type_name': "$type_name.name",
                    'type_value': "$type_value.name",
                }
            }
        }
    }, {
            '$project': {
            "id": '$_id',
            "_id": 0,
            'product_id':1,
            'product_varient_id':1,
            'product_name':1,
            "sku_number":1,
            'retail_price':1,
            'dimensions':1,
            "finished_price":1,
            "un_finished_price":1,
            "quantity": 1,
            "image_url": 1,
            'varient_option_list':1
            }
        }
        ,{
            '$sort':{'id':1}
        }
    ]
    result = list(products.objects.aggregate(*pipeline))
    product_set = list()
    for i in result:
        del i['id']
        i['brand_id'] = str(json_req['brand_id'])
        product_set.append(i['product_id'])
        if i[price_option] == None:
            i[price_option] = 'finished_price'
        i[price_option] = float(i[price_option])
        if price_option == 'finished_price':
            json_req['price'] = float(json_req['price'])
            i['price'] = str(json_req['price'])
            i['price_adding_sympol'] = json_req['price_symbol']
            if json_req['price_symbol'] == '%':
                i[price_option] = i[price_option] + (i[price_option]/100*json_req['price'])
            else:
                i[price_option] = float(i[price_option]) + (json_req['price'])
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'product_id':i['product_id']})
            i['product_id'] = str(i['product_id'])
            i['product_varient_id'] = str(i['product_varient_id'])
            category_id = ""
            if product_category_config_obj:
                category_id = product_category_config_obj.category_id
            i['category_id'] = product_category_config_obj.category_id
            brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':category_id,'brand_id':ObjectId(json_req['brand_id']),'price_option':price_option,'is_active':True})
            i['old_retail_price'] = i['retail_price'] 
            if brand_category_price_obj:
                i['retail_price'] =  str(i[price_option] * float(brand_category_price_obj.price))
            else:
                i['retail_price'] = str(i[price_option])
            i[price_option] = str(i[price_option])
            i['price_option'] = price_option
            i['option_value_id'] =json_req['option_value_id']
            i['option_name_id'] =json_req['option_name_id']
        elif price_option == 'un_finished_price':
            json_req['price'] = float(json_req['price'])
            i['finished_price'] = float(i['finished_price'])
            i['price'] = str(json_req['price'])
            i['price_adding_sympol'] = json_req['price_symbol']
            if json_req['price_symbol'] == '%':
                i['finished_price'] = i['finished_price'] + ((i[price_option])/100*json_req['price'])
            else:
                i['finished_price'] = float(i['finished_price']) + ((i[price_option]) + (json_req['price']))
            product_category_config_obj = DatabaseModel.get_document(product_category_config.objects,{'product_id':i['product_id']})
            i['product_id'] = str(i['product_id'])
            i['product_varient_id'] = str(i['product_varient_id'])
            category_id = ""
            if product_category_config_obj:
                category_id = product_category_config_obj.category_id
            i['category_id'] = product_category_config_obj.category_id
            brand_category_price_obj = DatabaseModel.get_document(brand_category_price.objects,{'category_id':category_id,'brand_id':ObjectId(json_req['brand_id']),'price_option':'finished_price','is_active':True})
            i['old_retail_price'] = i['retail_price'] 
            if brand_category_price_obj:
                i['retail_price'] =  str(i['finished_price'] * float(brand_category_price_obj.price))
            else:
                i['retail_price'] = str(i['finished_price'])
            i[price_option] = str(i[price_option])
            i['price_option'] = price_option
            i['option_value_id'] =json_req['option_value_id']
            i['option_name_id'] =json_req['option_name_id']
    data = dict()
    data['result'] = result
    data['product_count'] = len(set(product_set))
    return data

@csrf_exempt
def saveChangesForVarientOption(request):
    json_req = JSONParser().parse(request)
    result_list = json_req['result_list'] 
    data = dict()
    data['result_list'] = json_req['result_list']
    data['result_list'] = json_req['result_list']
    option_value_id_list = [ObjectId(i) for i in result_list[0]['option_value_id']]
    
    # revert_varient_retail_price_obj = DatabaseModel.get_document(revert_varient_retail_price.objects,{"brand_id" :ObjectId(brand_id),"type_name_id" :result_list[0]['option_name_id'],"type_value_id__in" :option_value_id_list,"current_price" :i['price'],'price_option':i['price_option']})   
    i = result_list[0]
    DatabaseModel.save_documents(revert_varient_retail_price,{"brand_id" :ObjectId(i['brand_id']),"type_name_id": i['option_name_id'],"type_value_id" :option_value_id_list,"current_price" :i['price'],'price_option':i['price_option'],'price_adding_sympol':i['price_adding_sympol']})
    for i in result_list:
        product_varient_obj = DatabaseModel.get_document(product_varient.objects,{'id':i['product_varient_id']})
        product_varient_obj.finished_price = str(i['finished_price'])
        product_varient_obj.un_finished_price = str(i['un_finished_price'])
        product_varient_obj.retail_price = str(i['retail_price'])
        # product_varient_obj.dimensions = str(i['dimensions'])
        product_varient_obj.save()
    return data

@csrf_exempt
def obtainRevertPreviousAndCurrentPriceForCategory(request):
    json_req = JSONParser().parse(request)
    data = dict()
    brand_category_price_list = DatabaseModel.list_documents(brand_category_price.objects,{'category_id__in':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'price_option':json_req['price_option']})
    data['old_price'] = 0
    data['current_price'] = 0
    last_two_values = []
    brand_category_price_list = list(brand_category_price_list) 
    if len(brand_category_price_list) >= 2:
        last_two_values = brand_category_price_list[-2:]
        data['old_price'] = last_two_values[0].price
        data['current_price'] = last_two_values[1].price
    elif len(brand_category_price_list) == 1:
        data['current_price'] = brand_category_price_list[0].price
        data['old_price'] = "0"
    return data


@csrf_exempt
def obtainRevertPreviousAndCurrentPriceForVarientOption(request):
    json_req = JSONParser().parse(request)
    brand_id = json_req.get('brand_id')
    type_name_id = json_req.get('option_name_id')
    type_value_id = json_req.get('option_value_id')
    type_value_id = [ObjectId(i) for i in type_value_id]
    data = dict()
    revert_varient_retail_price_obj = DatabaseModel.list_documents(revert_varient_retail_price.objects,{'brand_id':brand_id,'type_name_id':type_name_id,'type_value_id__in':type_value_id,'price_option':json_req['price_option']})
    revert_varient_retail_price_obj = list(revert_varient_retail_price_obj)
    revert_varient_retail_price_obj = list(revert_varient_retail_price_obj) 
    if len(revert_varient_retail_price_obj) >= 2:
        last_two_values = revert_varient_retail_price_obj[-2:]
        data['old_price'] = last_two_values[0].current_price
        data['current_price'] = last_two_values[1].current_price
    elif len(revert_varient_retail_price_obj) == 1:
        data['current_price'] = revert_varient_retail_price_obj[0].current_price
        data['old_price'] = "0"
    return data

@csrf_exempt
def updateRevertPriceForCategory(request):
    json_req = JSONParser().parse(request)
    brand_category_price_list = DatabaseModel.list_documents(brand_category_price.objects,{'category_id__in':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'price_option':json_req['price_option']})
    old_price = 0
    current_price = 0
    last_two_values = []
    brand_category_price_list = list(brand_category_price_list) 
    brand_category_price_obj_price = "1"
    if len(brand_category_price_list) >= 2:
        last_two_values = brand_category_price_list[-2:]
        old_price = last_two_values[0].price
        current_price = last_two_values[1].price
        brand_category_price_obj_1 = DatabaseModel.get_document(brand_category_price.objects,{'category_id__in':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'price':old_price,'price_option':json_req['price_option']})
        if brand_category_price_obj_1:
            brand_category_price_obj_1.is_active = True
            brand_category_price_obj_1.save()
            brand_category_price_obj_price = brand_category_price_obj_1.price
            
        brand_category_price_obj_2 = DatabaseModel.get_document(brand_category_price.objects,{'category_id__in':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'price':current_price,'price_option':json_req['price_option']})
        if brand_category_price_obj_2:
            brand_category_price_obj_2.delete()
    elif len(brand_category_price_list) == 1:
        current_price = brand_category_price_list[0].price
        old_price = brand_category_price_list[0].price
        brand_category_price_obj_1 = DatabaseModel.get_document(brand_category_price.objects,{'category_id__in':json_req['category_id'],'brand_id':ObjectId(json_req['brand_id']),'price':current_price,'price_option':json_req['price_option']})
        brand_category_price_obj_1.delete()
    pipeline = [
    {
            "$match":{'brand_id':ObjectId(json_req['brand_id'])}
        },
        {
            '$lookup': {
                "from": 'product_category_config',
                "localField": '_id',
                "foreignField": "product_id",
                "as": "product_category_config_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_category_config_ins',
                'preserveNullAndEmptyArrays': True
            }
        },{
            "$match":{'product_category_config_ins.category_id':{'$in':(json_req['category_id'])}}
        },
        
        {
            '$lookup': {
                "from": 'product_varient',
                "localField": 'options',
                "foreignField": "_id",
                "as": "product_varient_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_varient_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
        '$group': {
            "_id":"$product_varient_ins._id",
            "product_varient_id": { "$first": "$product_varient_ins._id" },
            "finished_price": { "$first": "$product_varient_ins.finished_price" },
            "un_finished_price": { "$first": "$product_varient_ins.un_finished_price" },
            "retail_price": { "$first": "$product_varient_ins.retail_price" },
            
        }
    }
    ]
    result = list(products.objects.aggregate(*pipeline))
    for i in result:
        if json_req['price_option'] == 'finished_price':
            retail_price = str(float(i['finished_price']) * float(brand_category_price_obj_price))
            DatabaseModel.update_documents(product_varient.objects,{'id':i['product_varient_id']},{'retail_price':retail_price})
        else:
            retail_price = str(float(i['un_finished_price']) * float(brand_category_price_obj_price))
            DatabaseModel.update_documents(product_varient.objects,{'id':i['product_varient_id']},{'retail_price':retail_price})
    data = dict()
    data['is_updated'] = True
    return data

@csrf_exempt
def updateRevertPriceForVarientOption(request):
    json_req = JSONParser().parse(request)
    type_value_id = json_req.get('option_value_id')
    type_value_id = [ObjectId(i) for i in type_value_id]
    revert_varient_retail_price_obj = DatabaseModel.list_documents(revert_varient_retail_price.objects,{'brand_id':json_req['brand_id'],'type_name_id':json_req['option_name_id'],'type_value_id__in':type_value_id,'price_option':json_req['price_option']})
    revert_varient_retail_price_obj = list(revert_varient_retail_price_obj)
    revert_varient_retail_price_obj = list(revert_varient_retail_price_obj) 
    brand_category_price_obj_price = "1"
    price_symbol = ""
    if len(revert_varient_retail_price_obj) >= 2:
        last_two_values = revert_varient_retail_price_obj[-2:]
        old_price = last_two_values[0].current_price
        current_price = last_two_values[1].current_price
        price_symbol = revert_varient_retail_price_obj[0].price_adding_sympol
        brand_category_price_obj_1 = DatabaseModel.get_document(revert_varient_retail_price.objects,
        {'brand_id':json_req['brand_id'],'type_name_id':json_req['option_name_id'],'type_value_id__in':type_value_id,'price_option':json_req['price_option'],'current_price':old_price})
        if brand_category_price_obj_1:
            brand_category_price_obj_price = brand_category_price_obj_1.current_price
        brand_category_price_obj_2 = DatabaseModel.get_document(revert_varient_retail_price.objects,
        {'brand_id':json_req['brand_id'],'type_name_id':json_req['option_name_id'],'type_value_id__in':type_value_id,'price_option':json_req['price_option'],'current_price':current_price})
        if brand_category_price_obj_2:
            brand_category_price_obj_2.delete()
    elif len(revert_varient_retail_price_obj) == 1:
        current_price = revert_varient_retail_price_obj[0].current_price
        price_symbol = revert_varient_retail_price_obj[0].price_adding_sympol
        old_price = "0"
        brand_category_price_obj_1 = DatabaseModel.get_document(revert_varient_retail_price.objects,
        {'brand_id':json_req['brand_id'],'type_name_id':json_req['option_name_id'],'type_value_id__in':type_value_id,'price_option':json_req['price_option'],'current_price':current_price})
        brand_category_price_obj_price = brand_category_price_obj_1.current_price
        
        brand_category_price_obj_1.delete()
    price_option = json_req['price_option']
    pipeline = [
    {
            "$match":{'brand_id':ObjectId(json_req['brand_id'])}
        },
        {
            '$lookup': {
                "from": 'product_varient',
                "localField": 'options',
                "foreignField": "_id",
                "as": "product_varient_ins"
            }
        }, {
            '$lookup': {
                "from": 'product_category_config',
                "localField": '_id',
                "foreignField": "product_id",
                "as": "product_category_config_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_category_config_ins',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$unwind': {
                'path': '$product_varient_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            '$lookup': {
                "from": 'product_varient_option',
                "localField": 'product_varient_ins.varient_option_id',
                "foreignField": "_id",
                "as": "product_varient_option_ins"
            }
        },
        {
            '$unwind': {
                'path': '$product_varient_option_ins',
                'preserveNullAndEmptyArrays': True
            }
        }, 
        {
            "$match":{'product_varient_option_ins.option_name_id':ObjectId(json_req['option_name_id']),'product_varient_option_ins.option_value_id':{'$in':type_value_id}}
        },
        {
        '$lookup': {
            'from': 'type_name',
            'localField': 'product_varient_option_ins.option_name_id',
            'foreignField': '_id',
            'as': 'type_name'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_name',
                'preserveNullAndEmptyArrays': True
            }
        },    {
        '$lookup': {
            'from': 'type_value',
            'localField': 'product_varient_option_ins.option_value_id',
            'foreignField': '_id',
            'as': 'type_value'
        }
        }, 
        {
            '$unwind': {
                'path': '$type_value',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
        '$group': {
            "_id":"$product_varient_ins._id",
            "product_id": { "$first": "$_id" },
            "category_id": { "$first": "$product_category_config_ins.category_id" },
            "product_varient_id": { "$first": "$product_varient_ins._id" },
            "product_name": { "$first": "$product_name" },
            "sku_number": { "$first": "$product_varient_ins.sku_number" },
            "finished_price": { "$first": "$product_varient_ins.finished_price" },
            "un_finished_price": { "$first": "$product_varient_ins.un_finished_price" },
            "retail_price": { "$first": "$product_varient_ins.retail_price" },
            "image_url": { "$first": "$image" },
            'varient_option_list': {
                "$push": {
                    'type_name': "$type_name.name",
                    'type_value': "$type_value.name",
                }
            }
        }
    }, {
            '$project': {
            "_id": 0,
            'product_id':1,
            'category_id':1,
            'product_varient_id':1,
            'product_name':1,
            "sku_number":1,
            'retail_price':1,
            "finished_price":1,
            "un_finished_price":1,
            "quantity": 1,
            "image_url": 1,
            'varient_option_list':1
            }
        }
    ]
    result = list(products.objects.aggregate(*pipeline))
    
    for i in result:
        i[price_option] = float(i[price_option])
        if price_symbol == '%':
            i[price_option] = i[price_option]/(1+(float(brand_category_price_obj_price)/100.00))
        else:
            i[price_option] = float(i[price_option]) - float(brand_category_price_obj_price)
        brand_category_price_obj_1 = DatabaseModel.get_document(brand_category_price.objects,{'category_id__in':[i['category_id']],'brand_id':ObjectId(json_req['brand_id']),'is_active':True,'price_option':price_option})
        if json_req['price_option'] == 'finished_price':
            retail_price = str(float(i[price_option]) * float(brand_category_price_obj_1.price))
            DatabaseModel.update_documents(product_varient.objects,{'id':i['product_varient_id']},{"finished_price":str(i[price_option]),'retail_price':retail_price})
        else:
            retail_price = str(float(i[price_option]) * float(brand_category_price_obj_1.price))
            DatabaseModel.update_documents(product_varient.objects,{'id':i['product_varient_id']},{"un_finished_price":str(i[price_option]),'retail_price':retail_price})
    data = dict()
    data['is_updated'] = True
    return data


def obtainUserBasedOnClient(request):
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    client_id = get_current_client()
    user_obj_list  = DatabaseModel.list_documents(user.objects,{'client_id':client_id,'id__ne':user_login_id})
    data = dict()
    data['user_list'] = list()
    for i in user_obj_list:
        data['user_list'].append({'id':str(i['id']),'name':i['name'],'role':i['role'],'is_active':i['is_active'] if 'is_active' in i else False })
    return data

@csrf_exempt
def UpdateProductActiveInActive(request):
    try:
        json_req = JSONParser().parse(request)
        product_id = json_req['id']
        new_status = json_req['is_active']
        products.objects(id=product_id).update_one(set__is_active=new_status)
        product_obj = products.objects.get(id=product_id)
        option_ids = [opt.id for opt in product_obj.options]
        product_varient.objects(id__in=option_ids).update(set__is_active=new_status)
        updated_product = products.objects.get(id=product_id)
        print("Full object:", updated_product.to_mongo())
        return {'is_update': True}
    except Exception as e:
        print(f" Error occured while updating product status",{e})

@csrf_exempt
def UpdateVarientActiveInActive(request):
    json_req = JSONParser().parse(request)
    product_varient_obj = DatabaseModel.get_document(product_varient.objects,{'id':json_req['id']})
    if product_varient_obj:
        product_varient_obj.is_active = json_req['is_active']
        product_varient_obj.save()
    data = dict()
    data['is_update'] = True
    return data

@csrf_exempt
def obtainInActiveProducts(request):
    client_id = get_current_client()
    search_term = request.GET.get('search')
    if search_term == None:
        search_term = ""
    pipeline = [
        
        {
        '$lookup': {
            'from': 'products',
            'localField': 'product_id',
            'foreignField': '_id',
            'as': 'products'
        }
    }, 
    {
            '$unwind': {
                'path': '$products',
                # 'preserveNullAndEmptyArrays': True
            }
        }, {
        "$match":{'products.is_active':False}
    },
    {
        "$match":{'products.client_id':ObjectId(client_id)}
    },
    {
        '$lookup': {
            'from': 'brand',
            'localField': 'products.brand_id',
            'foreignField': '_id',
            'as': 'brand'
        }
    }, 
    {
            '$unwind': {
                'path': '$brand',
                'preserveNullAndEmptyArrays': True
            }
        },
    {
        '$group': {
            "_id":'$_id',
            # 'product_list': {
            #     "$push": {
            'product_name':{'$first': "$products.product_name"},
            'product_id':{'$first': "$products._id"},
            'model':{'$first':"$products.model"},
            'upc_ean':{'$first':"$products.upc_ean"},
            'is_active':{'$first':"$products.is_active"},
            'breadcrumb':{'$first':"$products.breadcrumb"},
            'brand':{'$first':"$brand.name"},
            'long_description':{'$first':"$products.long_description"},
            'short_description':{'$first':"$products.short_description"},
            'features':{'$first':"$products.features"},
            'attributes':{'$first':"$products.attributes"},
            'tags':{'$first':"$products.tags"},
            'msrp':{'$first':"$products.msrp"},
            'mpn':{'$first':"$products.mpn"},
            'base_price':{'$first':"$products.base_price"},
            'key_features':{'$first':"$products.key_features"},
            'image':{'$first':"$products.image"},
            'level':{'$first':'$category_level'},
            'category_id':{'$first':'$category_id'}
                }
            },{
    '$match': {
    '$or': [
        { 'upc_ean': { '$regex': search_term, '$options': 'i' } },  
        { 'short_description': { '$regex': search_term, '$options': 'i' } }, 
        { 'mpn': { '$regex': search_term, '$options': 'i' } },  
        { 'product_name': { '$regex': search_term, '$options': 'i' } },
        { 'brand': { '$regex': search_term, '$options': 'i' } }, 
        { 'model': { '$regex': search_term, '$options': 'i' } },
        { 'features': { '$regex': search_term, '$options': 'i' } },
]
    }
  }
        # }
    # }
    ]
    result = list(product_category_config.objects.aggregate(*pipeline))
    data = dict()
    for j in result:
        del (j['_id'])
        j['product_id'] = str(j['product_id']) if 'product_id'in j else ""
        getCategoryLevelOrder(j)
    data['product_list'] = result
    data['product_count'] = len(result)
    return data

from functools import lru_cache
from bson import ObjectId

@lru_cache(maxsize=1024)
def get_root_category_name(level_one_id):
    """Safely get root category name with caching"""
    root = DatabaseModel.get_document(
        category.objects,
        {'level_one_category_list__in': [ObjectId(level_one_id)]}
    )
    return root.name if root else "Unknown Category"

def build_breadcrumb(category_id, level):
    """Build full breadcrumb safely and efficiently"""
    breadcrumb = []
    
    # Map level string to model and parent field lookup
    level_config = {
        'level-1': (category, None, None),
        'level-2': (level_one_category, 'level_one_category_list__in', None),
        'level-3': (level_two_category, 'level_two_category_list__in', 'level_one_category_list__in'),
        'level-4': (level_three_category, 'level_three_category_list__in', 'level_two_category_list__in'),
        'level-5': (level_four_category, 'level_four_category_list__in', 'level_three_category_list__in'),
        'level-6': (level_five_category, 'level_five_category_list__in', 'level_four_category_list__in'),
    }
    
    if level not in level_config:
        return "Invalid Level", "Invalid Level"

    model, parent_field, grandparent_field = level_config[level]
    
    # Get current level object
    obj = DatabaseModel.get_document(model.objects, {'id': ObjectId(category_id)})
    if not obj:
        return "Deleted Category", "Deleted Category"
    
    current_name = getattr(obj, 'name', 'Unknown')
    category_number = getattr(obj, 'category_number', '')
    
    # Walk up to root
    current_id = str(obj.id)
    current_level_num = int(level.split('-')[1])
    
    while current_level_num > 1:
        prev_level_num = current_level_num - 1
        prev_model = level_config[f'level-{prev_level_num}'][0]
        field_name = level_config[f'level-{current_level_num}'][1]
        
        parent = DatabaseModel.get_document(
            prev_model.objects,
            {field_name: [ObjectId(current_id)]}
        )
        if not parent:
            break
        breadcrumb.append(parent.name)
        current_id = str(parent.id)
        current_level_num -= 1
    
    # Add root category (level-0)
    # if current_level_num == 1:
    #     root_name = get_root_category_name(current_id)
    #     breadcrumb.append(root_name)
    
    breadcrumb.reverse()
    full_path = " > ".join(breadcrumb + [current_name])
    last_name = current_name
    
    return full_path, last_name, category_number


def obtainListofCategoryCombinations(request):
    client_id = get_current_client()
    if not client_id:
        return {'last_all_ids': []}
    
    last_all_ids = []
    
    # Step 1: Get all leaf categories efficiently
    categories = DatabaseModel.list_documents(
        category.objects,
        {'client_id': ObjectId(client_id)}
    )
    
    for cat in categories:
        def traverse(obj, path=None):
            if path is None:
                path = []
            
            # Go as deep as possible
            children = []
            if hasattr(obj, 'level_one_category_list'):
                children = obj.level_one_category_list
            elif hasattr(obj, 'level_two_category_list'):
                children = obj.level_two_category_list
            # ... add more if needed
            
            if not children:
                # Leaf node
                last_all_ids.append({
                    'id': str(obj.id),
                    'level': f'level-{len(path) + 1}'
                })
                return
            
            for child in children:
                traverse(child, path + [obj])
        
        traverse(cat)
    
    # Step 2: Build breadcrumbs safely and fast
    result = []
    for item in last_all_ids:
        full_name, last_name, cat_number = build_breadcrumb(item['id'], item['level'])
        
        result.append({
            'id': item['id'],
            'category_name': full_name,
            'category_last_name': last_name,
            'category_number': cat_number,
            'category_level_str': item['level']
        })
    
    return {'last_all_ids': result}

@csrf_exempt
def updatevarientToReleatedCategories(request):
    json_req = JSONParser().parse(request)
    category_id_list = json_req['category_id_list']
    varient_option_id =  json_req['varient_option_id']
    category_level =  json_req['category_level']
    type_name =  json_req['type_id']
    type_value_list =  json_req['option_value_list']
    type_value_list = [ObjectId(i['type_value_id']) for i in type_value_list]
    for i in category_id_list:
        category_varient_obj = DatabaseModel.get_document(category_varient.objects,{'category_id':i})
        is_present = False
        if category_varient_obj:
            for i in category_varient_obj.varient_option_id_list:
                if str(i.option_name_id.id) == type_name:
                    i.option_value_id_list.extend(type_value_list)
                    i.save()
                    is_present = True
                    break
            if is_present == False:
                option_name_id = DatabaseModel.get_document(varient_option.objects,{'id':varient_option_id}).option_name_id.id
                varient_option_obj = DatabaseModel.save_documents(varient_option,{'option_name_id':option_name_id,'option_value_id_list':type_value_list})
                category_varient_obj.varient_option_id_list.append(ObjectId(varient_option_obj.id))
                category_varient_obj.save()
        else:
            option_name_id = DatabaseModel.get_document(varient_option.objects,{'id':varient_option_id}).option_name_id.id
            varient_option_id = DatabaseModel.save_documents(varient_option,{'option_name_id':option_name_id,'option_value_id_list':type_value_list}).id
            category_varient_obj = DatabaseModel.save_documents(category_varient,{'category_id':i,'varient_option_id_list':[varient_option_id],'category_level':category_level})
    data = dict()
    data['is_updated']  = True
    return data

@csrf_exempt
def updateTaxonomyForProduct(request):
    json_req = JSONParser().parse(request)
    data  = dict()
    DatabaseModel.update_documents(product_category_config.objects,{'product_id':json_req['product_id']},{'category_id':json_req['category_id'],'category_level':json_req['category_level']})
    return data
    

@csrf_exempt
def cloneProduct(request):
    json_req = JSONParser().parse(request)
    product_id = json_req['id']
    client_id = get_current_client()
    product_obj = DatabaseModel.get_document(products.objects, {'id': product_id,'client_id':client_id})
    product_category_config_obj = DatabaseModel.get_document(product_category_config.objects, {'product_id': product_id})
    count = DatabaseModel.count_documents(products.objects, {'product_name__startswith':product_obj.product_name,"client_id":client_id,'mpn':product_obj.mpn})
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    count = count - 1
    for i in product_obj.options:
        for j in i.varient_option_id:
            j.pk = None
            j.save()
        i.pk = None
        i.sku_number = f"{i.sku_number} (Copy)"
        i.save()
        logForCreateProductVarient(i.id,user_login_id,"clone",{})
    product_obj.pk = None
    if count <1:
        product_obj.product_name = f"{product_obj.product_name} (Copy)"
    else:
        product_obj.product_name = f"{product_obj.product_name} (Copy {count})"  
    product_category_config_obj.pk = None
    product_obj.save()
    product_category_config_obj.product_id = product_obj.id
    product_category_config_obj.save()
    print(f"Cloned product created with new ID: {product_obj.id}")
    data = dict()
    logForCreateProduct(product_id,user_login_id,"clone",{})

    data['is_created'] = True
    return data

@csrf_exempt
def cloneVarient(request):
    json_req = JSONParser().parse(request)
    product_id = json_req['id']
    user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
    variant_id = json_req['variant_id']
    client_id = get_current_client()
    product_obj = DatabaseModel.get_document(products.objects, {'id': product_id,'client_id':client_id})
    option_list = []
    for i in product_obj.options:
        option_list.append(i.id)
        if str(i.id) == variant_id:
            for j in i.varient_option_id:
                j.pk = None
                j.save()
            i.pk = None
            i.sku_number = f"{i.sku_number} (Copy)"
            i.save()
            option_list.append(i.id)
            logForCreateProductVarient(i.id,user_login_id,"clone",{})

    product_obj.options=(option_list)
    product_obj.save()
    data = dict()
    data['is_created'] = True
    return data

@csrf_exempt
def brandUpdate(request):
    json_req = JSONParser().parse(request)
    brand_id = json_req['update_obj']['id']
    del json_req['update_obj']['product_count']
    del json_req['update_obj']['sku_count']
    DatabaseModel.update_documents(brand.objects,{'id':brand_id},json_req['update_obj'])
    data = dict()
    data['is_updated'] = True
    return data

from django.core.cache import cache
import time

def obtainVarientOptions(request):
    print('request', obtainVarientOptions)
    start_time = time.time()
    
    category_id = request.GET.get("id")
    client_id = get_current_client()
    
    # Build match condition
    match_condition = {'category_id': {'$ne': 'all'}}
    if category_id:
        match_condition['category_id'] = category_id
    
    # Optimized aggregation pipeline
    pipeline = [
        {'$match': match_condition},
        {
            '$lookup': {
                'from': 'varient_option',
                'let': {'varient_ids': '$varient_option_id_list'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$in': ['$_id', '$$varient_ids']},
                        'client_id': ObjectId(client_id)
                    }}
                ],
                'as': 'varient_option'
            }
        },
        {'$unwind': {'path': '$varient_option', 'preserveNullAndEmptyArrays': True}},
        {
            '$lookup': {
                'from': 'type_name',
                'localField': 'varient_option.option_name_id',
                'foreignField': '_id',
                'as': 'type_name'
            }
        },
        {'$unwind': {'path': '$type_name', 'preserveNullAndEmptyArrays': True}},
        {
            '$lookup': {
                'from': 'type_value',
                'localField': 'varient_option.option_value_id_list',
                'foreignField': '_id',
                'as': 'type_value'
            }
        },
        {'$unwind': {'path': '$type_value', 'preserveNullAndEmptyArrays': True}},
        {
            '$group': {
                    "_id": "$varient_option._id",
                    "type_name": {"$first": "$type_name.name"},
                    "varient_option_id": {"$first": "$varient_option._id"}, 
                    "type_id": {"$first": "$type_name._id"},
                    "category_id": {"$first": "$category_id"},
                    "option_value_list": {
            '$push': { 
                'type_value_name': "$type_value.name",
                'type_value_id': {"$toString": "$type_value._id"} 
            }
        }
            }
        },
        {
            '$project': {
                "_id": 0,
                "type_name": 1,
                "varient_option_id": {"$toString": "$varient_option_id"},
                "type_id": {"$toString": "$type_id"},
                "category_id": {"$toString": "$category_id"},
                'option_value_list': 1,
            }
        }
    ]
    
    try:
        # Execute with timeout
        result = list(category_varient.objects.aggregate(pipeline, allowDiskUse=True))
        
        data = {'category_varient_id': "", 'varient_list': []}
        
        if result:
            # BATCH FETCH category names to avoid N+1 queries
            if not category_id:  # Only needed when no specific category filter
                category_names_map = fetch_category_names_batch(result)
            else:
                category_names_map = {}
            
            processed_results = []
            for i in result:
                
                # Add category name if available
                if not category_id and 'category_id' in i:
                    category_name = category_names_map.get(str(i['category_id']))
                    if category_name:
                        i['type_name'] = f"{i.get('type_name', '')} ({category_name})"
                
                # Clean up and convert IDs
                
                
                # Process option values
                # option_values = i.get('option_value_list', [])
                # for j in option_values:
                #     j['type_value_id'] = str(j.get('type_value_id', ''))
                
                processed_results.append(i)
            
            data['varient_list'] = processed_results
        
        execution_time = time.time() - start_time
        print(f"obtainVarientOptions executed in {execution_time:.2f} seconds")
        
        return data
        
    except Exception as e:
        print(f"Error in obtainVarientOptions: {str(e)}")
        return {'category_varient_id': "", 'varient_list': []}


def fetch_category_names_batch(result_items):
    """Batch fetch category names to avoid N+1 queries"""
    category_ids = set()
    for item in result_items:
        if 'category_id' in item:
            category_ids.add(item['category_id'])
    
    category_names_map = {}
    collections = [
        (category, 'category'),
        (level_five_category, 'level_five'),
        (level_four_category, 'level_four'),
        (level_three_category, 'level_three'),
        (level_two_category, 'level_two'),
        (level_one_category, 'level_one')
    ]
    
    for collection, coll_name in collections:
        try:
            # Batch query all categories at once
            category_objs = collection.objects(id__in=list(category_ids))
            for cat in category_objs:
                category_names_map[str(cat.id)] = getattr(cat, 'name', 'Unknown')
        except Exception as e:
            print(f"Error fetching from {coll_name}: {e}")
            continue
    
    return category_names_map

@csrf_exempt
def updateCategoryToProducts(request):
    payload = json.loads(request.body)
    product_ids = payload.get('product_ids', [])
    category_id = payload.get('category_id', '')
    category_level = payload.get('category_level')
    if not product_ids or not category_id:
        return JsonResponse({'error': 'Missing product_ids or new_category_name'}, status=400)
    
    obj_product_ids = []
    for pid in product_ids:
        obj_product_ids.append(ObjectId(pid))

    updated_count = product_category_config.objects(product_id__in=obj_product_ids).update(
        category_id=category_id,
        category_level= category_level
    )

    
    return JsonResponse({
            "status": True,
            "emessage": f"Category updated for {updated_count} products",
            "updated_count": updated_count
        })