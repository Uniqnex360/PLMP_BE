from .global_service import DatabaseModel
from .models import category
from .models import level_one_category
from .models import level_two_category
from .models import level_three_category
from .models import level_four_category
from .models import level_five_category

def getCategoryLevelOrder(i):
    if not i.get('category_id') or i['category_id'] == "None":
        return

    cid = i['category_id']
    level = i['level']
    path_str = ""
    leaf_name = ""
    cat_number = ""

    # --- Level 1 ---
    if level == 'level-1':
        obj = category.objects(id=cid).first()
        if obj:
            leaf_name = obj.name
            cat_number = obj.category_number
            path_str = obj.name

    # --- Level 2 ---
    elif level == 'level-2':
        obj = level_one_category.objects(id=cid).first()
        if obj and obj.parent_id:
            leaf_name = obj.name
            cat_number = obj.category_number
            # Directly access the specific parent
            path_str = f"{obj.parent_id.name} > {obj.name}"

    # --- Level 3 ---
    elif level == 'level-3':
        obj = level_two_category.objects(id=cid).first()
        if obj and obj.parent_id and obj.parent_id.parent_id:
            leaf_name = obj.name
            cat_number = obj.category_number
            # Walk up: Parent -> Grandparent
            l1 = obj.parent_id
            root = l1.parent_id
            path_str = f"{root.name} > {l1.name} > {obj.name}"

    # --- Level 4 ---
    elif level == 'level-4':
        obj = level_three_category.objects(id=cid).first()
        if obj and obj.parent_id:
            leaf_name = obj.name
            cat_number = obj.category_number
            
            l2 = obj.parent_id
            if l2.parent_id:
                l1 = l2.parent_id
                if l1.parent_id:
                    root = l1.parent_id
                    path_str = f"{root.name} > {l1.name} > {l2.name} > {obj.name}"

    # --- Level 5 ---
    elif level == 'level-5':
        obj = level_four_category.objects(id=cid).first()
        if obj and obj.parent_id:
            leaf_name = obj.name
            cat_number = obj.category_number
            
            l3 = obj.parent_id
            if l3.parent_id:
                l2 = l3.parent_id
                if l2.parent_id:
                    l1 = l2.parent_id
                    if l1.parent_id:
                        root = l1.parent_id
                        path_str = f"{root.name} > {l1.name} > {l2.name} > {l3.name} > {obj.name}"

    # --- Level 6 ---
    elif level == 'level-6':
        obj = level_five_category.objects(id=cid).first()
        if obj and obj.parent_id:
            leaf_name = obj.name
            cat_number = obj.category_number
            
            l4 = obj.parent_id
            if l4.parent_id:
                l3 = l4.parent_id
                if l3.parent_id:
                    l2 = l3.parent_id
                    if l2.parent_id:
                        l1 = l2.parent_id
                        if l1.parent_id:
                            root = l1.parent_id
                            path_str = f"{root.name} > {l1.name} > {l2.name} > {l3.name} > {l4.name} > {obj.name}"

    # --- Assignment ---
    i['category_name'] = path_str
    i['category_last_name'] = leaf_name
    i['category_number'] = cat_number