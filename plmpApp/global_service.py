import redis # type: ignore
import pickle
from django.db import models # type: ignore
from mongoengine.queryset import QuerySet   # type: ignore
from bson import ObjectId

class DatabaseModel:
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=False)
    @staticmethod
    def get_document(queryset, filter={}, field_list=[]):
        try:
            if isinstance(queryset, QuerySet):
                queryset_name = queryset._document.__name__
                if queryset_name  in ['user','category','level_one_category','level_two_category','level_three_category','level_four_category','level_five_category','type_name','type_value','vendor','capability' ]:
                    cache_key = f"{queryset_name}:get:{str(filter)}:{str(field_list)}"
                    cached_data = DatabaseModel.redis_client.get(cache_key)
                    if cached_data:
                        return pickle.loads(cached_data)
            data = queryset.filter(**filter).only(*field_list).limit(1)
            if data:
                data = data[0]
                if queryset_name in []:
                    DatabaseModel.redis_client.setex(cache_key, 3600, pickle.dumps(data))
                return data
            else:
                return None
        except Exception as e:
            # print(f"Error occurred while fetching document: {e}")
            return None

    @staticmethod
    def list_documents(queryset, filter={}, field_list=[], sort_list=[], lower_limit=None, upper_limit=None):
        try:
            # if isinstance(queryset, QuerySet):
            #     queryset_name = queryset._document.__name__
            #     cache_key = f"{queryset_name}:list:{str(filter)}:{str(field_list)}"
            #     cached_data = DatabaseModel.redis_client.get(cache_key)
            #     if cached_data:
            #         return pickle.loads(cached_data)
            data = queryset(**filter).skip(lower_limit).limit(upper_limit - lower_limit if lower_limit is not None and upper_limit is not None else None).only(*field_list).order_by(*sort_list)
            if data:
                # DatabaseModel.redis_client.setex(cache_key, 3600, pickle.dumps(data))
                return data
            else:
                return []  
        except Exception as e:
            # print(f"Error occurred while fetching documents: {e}")
            return []

    @staticmethod
    def update_documents(queryset, filter={}, json={}):
        try:
            # Fetch the existing document
            existing_doc = queryset.filter(**filter).first()
            if not existing_doc:
                # print("No matching document found.")
                return False, {}

            updated_fields = {}
            change_log = {}

            def is_reference_field(value):
                if isinstance(value, ObjectId):
                    return True
                if isinstance(value, list) and all(isinstance(i, ObjectId) for i in value):
                    return True
                return False

            for key, new_value in json.items():
                old_value = getattr(existing_doc, key, None)

                # Skip if both values are None
                if old_value is None and new_value is None:
                    continue

                # Skip if it's a reference field
                if is_reference_field(old_value) or is_reference_field(new_value):
                    continue

                # Skip if values are the same
                if old_value == new_value:
                    continue

                updated_fields[key] = new_value
                change_log[key] = f"{old_value} -> {new_value}"

            # if not updated_fields:
            #     print("No valid fields to update — all values are the same, None, or references.")
                # return False, {}

            # print(f"Fields being updated: {change_log}")

            # Perform the update
            data = queryset.filter(**filter).update(**json)

            # Clear Redis cache if update is successful
            if data and isinstance(queryset, QuerySet):
                queryset_name = queryset._document.__name__
                pattern = f"{queryset_name}:*"
                keys = DatabaseModel.redis_client.keys(pattern)
                if keys:
                    DatabaseModel.redis_client.delete(*keys)
            return bool(data), change_log

        except Exception as e:
            # print(f"Error occurred while updating documents: {e}")
            return False, {}

    
    def save_documents(queryset,  json={}):
        obj = queryset(**json)
        obj.save()
        return obj

    def delete_documents(queryset,  json={}):
        queryset(**json).delete()
        return True
    
    def count_documents(queryset,filter={}):
        count = queryset(**filter).count()
        return count