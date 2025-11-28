from mongoengine import Document , fields,EmbeddedDocument # type: ignore
from datetime import datetime
from bson import ObjectId # type: ignore
import random
import string

class manufacture(Document):
    name = fields.StringField(required=True)
    logo = fields.StringField(required=True)
class client(Document):
    name = fields.StringField(required=True)
    logo = fields.StringField()
    location = fields.StringField()


class user(Document):
    name = fields.StringField(required=True)
    email = fields.StringField(required=True)
    user_name = fields.StringField()
    role = fields.StringField()
    password = fields.StringField()
    client_id = fields.ReferenceField(client)
    is_active = fields.BooleanField(default=False)
    def generate_default_username(self):
        random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
        random_password = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
        self.user_name = f"{self.name[:5].lower()}{random_str}"
        self.password = random_password

    def save(self, *args, **kwargs):
        if not self.user_name: 
            self.generate_default_username()
    
        from .custom_middleware import get_current_client
        client_id = get_current_client()  # should return the client ObjectId or client document

        if client_id:
            from .models import client  # ensure you import the client Document
        # If get_current_client() returns ObjectId
            if isinstance(client_id, ObjectId):
                self.client_id = client.objects(id=client_id).first()
        # If it already returns a client document
            else:
                self.client_id = client_id

        return super(user, self).save(*args, **kwargs)


class brand_count(Document):
    client_id = fields.ReferenceField(client)
    brand_count_int = fields.IntField()

class brand(Document):
    brand_number = fields.StringField()
    name = fields.StringField()
    email = fields.StringField()
    mobile_number = fields.StringField() 
    address = fields.StringField() 
    city = fields.StringField() 
    state = fields.StringField()
    zip_code = fields.StringField()
    # phone_number = fields.StringField()
    website  = fields.StringField()
    number_of_feeds  = fields.StringField()
    logo = fields.StringField()
    client_id = fields.ReferenceField(client)
    
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        brand_count_obj = DatabaseModel.get_document(brand_count.objects,{'client_id':client_id})
        brand_number_var = 0
        if brand_count_obj:
            brand_count_obj.brand_count_int += 1
            brand_number_var = brand_count_obj.brand_count_int
            brand_count_obj.save()
        else:
            DatabaseModel.save_documents(brand_count,{'client_id':client_id,"brand_count_int":1})
            brand_number_var = 1
        self.brand_number = 'BR-'+str(brand_number_var)
        self.client_id = ObjectId(client_id)
        return super(brand, self).save(*args, **kwargs)
    
class category_count(Document):
    client_id = fields.ReferenceField(client)
    category_count_int = fields.IntField()
    
class level_five_category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    client_id = fields.ReferenceField(client)
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        self.category_number = 'CAT-6-'+str(category_number_var)
        return super(level_five_category, self).save(*args, **kwargs)

class level_four_category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    client_id = fields.ReferenceField(client)
    level_five_category_list = fields.ListField(fields.ReferenceField(level_five_category),default = [])
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        self.category_number = 'CAT-5-'+str(category_number_var)
        return super(level_four_category, self).save(*args, **kwargs)

class level_three_category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    client_id = fields.ReferenceField(client)
    
    level_four_category_list = fields.ListField(fields.ReferenceField(level_four_category),default = [])
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        self.client_id = ObjectId(client_id)
        self.category_number = 'CAT-4-'+str(category_number_var)
        return super(level_three_category, self).save(*args, **kwargs)

class level_two_category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    client_id = fields.ReferenceField(client)
    
    level_three_category_list = fields.ListField(fields.ReferenceField(level_three_category),default = [])
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        self.client_id = ObjectId(client_id)
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        self.category_number = 'CAT-3-'+str(category_number_var)
        return super(level_two_category, self).save(*args, **kwargs)

class level_one_category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    client_id = fields.ReferenceField(client)
    level_two_category_list = fields.ListField(fields.ReferenceField(level_two_category),default = [])
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        self.category_number = 'CAT-2-'+str(category_number_var)
        return super(level_one_category, self).save(*args, **kwargs)

class category(Document):
    meta = {
        'indexes': [
            {
                'fields': ['name', 'client_id'],
                'unique': True
            }
        ]
    }
    name = fields.StringField(required=True)
    category_number = fields.StringField()
    level_one_category_list = fields.ListField(fields.ReferenceField(level_one_category),default = [])
    level_two_category_list = fields.ListField(
        fields.ReferenceField(level_two_category), 
        default=[]
    )
    client_id = fields.ReferenceField(client)
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .models import category_count
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
            
        self.category_number = 'CAT-1-'+str(category_number_var)
        return super(category, self).save(*args, **kwargs)
class type_name(Document):
    name = fields.StringField(required=True)


class type_value(Document):
    name = fields.StringField()
    images = fields.ListField(fields.StringField())


class varient_option(Document):
    option_name_id = fields.ReferenceField(type_name)
    option_value_id_list = fields.ListField(fields.ReferenceField(type_value),default = [])
    client_id = fields.ReferenceField(client)
    category_str = fields.StringField()
    def save(self, *args, **kwargs):
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        return super(varient_option, self).save(*args, **kwargs)
class product_varient_option(Document):
    option_name_id = fields.ReferenceField(type_name)
    option_value_id = fields.ReferenceField(type_value)

class product_varient(Document):
    sku_number = fields.StringField()
    varient_option_id = fields.ListField(fields.ReferenceField(product_varient_option))
    image_url = fields.ListField(fields.StringField())
    finished_price = fields.StringField()
    un_finished_price = fields.StringField()
    quantity = fields.StringField()
    is_active = fields.BooleanField(default = True)
    client_id = fields.ReferenceField(client)
    total_price = fields.StringField()
    retail_price = fields.StringField(default="0")
    # dimensions = fields.StringField()
    def save(self, *args, **kwargs):
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        return super(product_varient, self).save(*args, **kwargs)
class dimensions(EmbeddedDocument):
    height = fields.StringField()
    width = fields.StringField()
    depth = fields.StringField()
    length = fields.StringField()

class products(Document):
    model = fields.StringField()
    mpn = fields.StringField()
    upc_ean = fields.StringField(default = "")
    breadcrumb = fields.StringField()
    brand_id = fields.ReferenceField(brand)
    product_name = fields.StringField()
    long_description = fields.StringField()
    short_description = fields.StringField()
    features = fields.StringField()
    attributes = fields.StringField()
    tags = fields.StringField()
    msrp = fields.StringField()
    # dimensions = fields.EmbeddedDocumentField(dimensions)
    features_notes = fields.StringField()
    option_str = fields.StringField()
    units = fields.StringField()
    is_active = fields.BooleanField(default = True)
    base_price = fields.StringField()
    discount_price = fields.StringField(default = "0")
    dealer_price = fields.StringField(default = "0")
    key_features = fields.StringField()
    options = fields.ListField(fields.ReferenceField(product_varient))
    image = fields.ListField(fields.StringField())
    client_id = fields.ReferenceField(client)
    dimensions = fields.StringField()

    def save(self, *args, **kwargs):
        from .models import price_log
        from .custom_middleware import get_current_user
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        user_login_id = get_current_user() 
        client_id = ObjectId(get_current_client())
        self.client_id = ObjectId(client_id)
        is_update = self.id is not None
        old_product = None
        if is_update:
            old_product = products.objects.get(id=self.id)
        super().save(*args, **kwargs)
        new_product = products.objects.get(id=self.id)
        pattern = f"{products}*"
        keys = DatabaseModel.redis_client.keys(pattern)
        if keys:
            DatabaseModel.redis_client.delete(*keys)
        if is_update and old_product:
            if old_product.base_price != self.base_price:
                price_log(
                    name=f"Base price",
                    product_id=self,
                    action="updated",
                    previous_price = old_product.base_price,
                    current_price = self.base_price,
                    user_id=ObjectId(user_login_id),
                    log_date=datetime.now()
                ).save()
            if old_product.msrp != self.msrp:
                price_log(
                    name=f"MSRP",
                    product_id=self,
                    action="updated",
                    previous_price = old_product.msrp,
                    current_price = self.msrp,
                    user_id=ObjectId(user_login_id),
                    log_date=datetime.now()
                ).save()
        elif not is_update:
            price_log(
                name=f"Base price",
                product_id=self,
                action="created",
                previous_price = "",
                current_price = self.base_price,
                user_id=ObjectId(user_login_id),
                log_date=datetime.now()
            ).save()
            price_log(
                name=f"MSRP",
                product_id=self,
                action="created",
                previous_price = "",
                current_price = self.msrp,
                user_id = ObjectId(user_login_id),
                log_date=datetime.now()
            ).save()


class price_log(Document):
    name = fields.StringField()
    product_id = fields.ReferenceField(products)
    action = fields.StringField()
    previous_price = fields.StringField()
    current_price = fields.StringField()
    user_id = fields.ReferenceField(user)
    log_date = fields.DateTimeField(default=datetime.now)


class leaf_option(EmbeddedDocument):
    leaf_count = fields.IntField()
    unfinished_price = fields.StringField()
    finished_price = fields.StringField()
    varient_code = fields.StringField()
    name = fields.StringField()


class ignore_calls(Document):
    name = fields.StringField()


class product_category_config(Document):
    product_id = fields.ReferenceField(products)
    category_level = fields.StringField()
    category_id = fields.StringField()


class vendor(Document):
    name = fields.StringField(required=True)
    manufacture = fields.StringField()


class category_varient(Document):
    category_id = fields.StringField()
    varient_option_id_list = fields.ListField(fields.ReferenceField(varient_option),default = [])
    category_level = fields.StringField()


class capability(Document):
    action_name = fields.StringField()
    role_list = fields.ListField(fields.StringField(),default = [])


class email_otp(Document):
    email = fields.EmailField(unique=True)
    otp = fields.StringField()
    expires_at = fields.DateTimeField()


    def __str__(self):
        return f'OTP for {self.email}'


class category_log(Document):
    category_id = fields.StringField()
    action = fields.StringField() 
    log_date = fields.DateTimeField(default=datetime.now)
    user_id = fields.ReferenceField(user)
    level = fields.StringField()
    client_id = fields.ReferenceField(client)
    data = fields.DictField()

class product_log(Document):
    product_id = fields.ReferenceField(products)
    action = fields.StringField()
    log_date = fields.DateTimeField(default=datetime.now)
    user_id = fields.ReferenceField(user)
    data = fields.DictField()

class product_varient_log(Document):
    product_varient_id = fields.ReferenceField(product_varient)
    action = fields.StringField()
    log_date = fields.DateTimeField(default=datetime.now)
    user_id = fields.ReferenceField(user)
    data = fields.DictField()



class category_varient_option_log(Document):
    category_id = fields.StringField()
    level = fields.StringField()
    category_varient_option_id = fields.ReferenceField(varient_option)
    action = fields.StringField()
    log_date = fields.DateTimeField(default=datetime.now)
    user_id = fields.ReferenceField(user)
    data = fields.DictField()

    
    
class xl_mapping(Document):
    data= fields.DictField()
    user_id = fields.ReferenceField(user)
    vendor_id = fields.ReferenceField(brand)

class brand_category_price(Document):
    brand_id = fields.ReferenceField(brand)
    category_id = fields.StringField()
    price = fields.StringField()
    is_active = fields.BooleanField(default=False)
    price_option = fields.StringField()
    
class radial_price_log(Document):
    product_varient_id = fields.ReferenceField(product_varient)
    old_retail_price = fields.StringField()
    new_retail_price = fields.StringField()
    user_id = fields.ReferenceField(user)
    log_date = fields.DateTimeField(default=datetime.now)
    client_id = fields.ReferenceField(client)
class revert_varient_retail_price(Document):
    brand_id = fields.ReferenceField(brand)
    type_name_id = fields.ReferenceField(type_name)
    type_value_id = fields.ReferenceField(type_value)
    current_price = fields.StringField()
    price_option = fields.StringField()
    price_adding_sympol = fields.StringField()
    