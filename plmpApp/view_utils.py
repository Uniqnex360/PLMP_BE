from .global_service import DatabaseModel
from .models import category
from .models import level_one_category
from .models import level_two_category
from .models import level_three_category
from .models import level_four_category
from .models import level_five_category

def getCategoryLevelOrder(i):
    if i['category_id'] != None and i['category_id'] != "None":
        if i['level']=='level-1':
            category_obj = DatabaseModel.get_document(category.objects,{'id':i['category_id']})
            if category_obj:
                i['category_name'] = category_obj.name
                i['category_last_name'] = category_obj.name
                i['category_number'] = category_obj.category_number
        elif i['level']=='level-2':
            level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'id':i['category_id']})
            if level_one_category_obj:
                i['category_name'] = level_one_category_obj.name
                i['category_last_name'] = level_one_category_obj.name
                i['category_number'] = level_one_category_obj.category_number
                i['category_name'] = DatabaseModel.get_document(category.objects,{'level_one_category_list__in':[i['category_id']]}).name + " > "+i['category_name']
        elif i['level']=='level-3':
            level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'id':i['category_id']})
            if level_two_category_obj:
                i['category_name'] = level_two_category_obj.name
                i['category_last_name'] = level_two_category_obj.name
                i['category_number'] = level_two_category_obj.category_number
                level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'level_two_category_list__in':[i['category_id']]})
                if level_one_category_obj:
                    i['category_name'] =  level_one_category_obj.name + " > " + i['category_name']
                    i['category_name'] = DatabaseModel.get_document(category.objects,{'level_one_category_list__in':[level_one_category_obj.id]}).name + " > " + i['category_name']
        elif i['level']=='level-4':
            level_three_category_obj = DatabaseModel.get_document(level_three_category.objects,{'id':i['category_id']})
            if level_three_category_obj:
                i['category_name'] = level_three_category_obj.name
                i['category_last_name'] = level_three_category_obj.name
                i['category_number'] = level_three_category_obj.category_number
                level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'level_three_category_list__in':[i['category_id']]})
                if level_two_category_obj:
                    i['category_name'] =  level_two_category_obj.name + " > " + i['category_name'] 
                    level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'level_two_category_list__in':[level_two_category_obj.id]})
                    if level_two_category_obj:
                        i['category_name'] =  level_one_category_obj.name + " > " + i['category_name'] 
                        i['category_name'] = DatabaseModel.get_document(category.objects,{'level_one_category_list__in':[level_one_category_obj.id]}).name + " > " + i['category_name'] 
        elif i['level']=='level-4':
            level_four_category_obj = DatabaseModel.get_document(level_four_category.objects,{'id':i['category_id']})
            if level_four_category_obj:
                i['category_name'] = level_four_category_obj.name
                i['category_last_name'] = level_four_category_obj.name
                i['category_number'] = level_four_category_obj.category_number
                level_three_category_obj = DatabaseModel.get_document(level_three_category.objects,{'level_four_category_list__in':[i['category_id']]})
                if level_three_category_obj:
                    i['category_name'] =  level_three_category_obj.name  + " > " + i['category_name']
                    level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'level_three_category_list__in':[level_three_category_obj.id]})
                    if level_two_category_obj:
                        i['category_name'] =  level_two_category_obj.name + " > " + i['category_name'] 
                        level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'level_two_category_list__in':[level_two_category_obj.id]})
                        if level_one_category_obj:
                            i['category_name'] =  level_one_category_obj.name + " > " + i['category_name']
                            i['category_name'] = DatabaseModel.get_document(category.objects,{'level_one_category_list__in':[level_one_category_obj.id]}).name  + " > " + i['category_name']
        elif i['level']=='level-6':
            level_five_category_obj = DatabaseModel.get_document(level_five_category.objects,{'id':i['category_id']})
            if level_five_category_obj:
                i['category_name'] = level_five_category_obj.name
                i['category_last_name'] = level_five_category_obj.name
                i['category_number'] = level_five_category_obj.category_number
                level_four_category_obj = DatabaseModel.get_document(level_four_category.objects,{'level_five_category_list__in':[i['category_id']]})
                if level_four_category_obj:
                    i['category_name'] =  level_four_category_obj.name  + " > " + i['category_name']
                    level_three_category_obj = DatabaseModel.get_document(level_three_category.objects,{'level_four_category_list__in':[level_four_category_obj.id]})
                    if level_three_category_obj:
                        i['category_name'] = level_three_category_obj.name  + " > " + i['category_name']
                        level_two_category_obj = DatabaseModel.get_document(level_two_category.objects,{'level_three_category_list__in':[level_three_category_obj.id]})
                        if level_two_category_obj:
                            i['category_name'] =level_two_category_obj.name + " > " +  i['category_name'] 
                            level_one_category_obj = DatabaseModel.get_document(level_one_category.objects,{'level_two_category_list__in':[level_two_category_obj.id]})
                            if level_one_category_obj:
                                i['category_name'] =level_one_category_obj.name   + " > " +  i['category_name']
                                i['category_name'] =DatabaseModel.get_document(category.objects,{'level_one_category_list__in':[level_one_category_obj.id]}).name + " > " + i['category_name']