from .global_service import DatabaseModel
from .models import category
from .models import level_one_category
from .models import level_two_category
from .models import level_three_category
from .models import level_four_category
from .models import level_five_category
from functools import lru_cache

# Optional: cache lookups to avoid hitting DB 1000 times for same object
@lru_cache(maxsize=512)
def get_category_by_id(model, obj_id):
    return DatabaseModel.get_document(model, {'id': obj_id})

def getCategoryLevelOrder(i):
    if not i.get('category_id') or i['category_id'] in (None, "None", ""):
        i['category_name'] = i.get('category_name', 'Uncategorized')
        i['category_last_name'] = i['category_name']
        i['category_number'] = ''
        return

    level = i['level']
    cat_id = i['category_id']

    # Start building breadcrumb: [Root > L1 > L2 > ... > Current]
    breadcrumb = []

    # Always try to get the root category (the top-most parent)
    root_cat = None

    if level == 'level-1':
        obj = get_category_by_id(category.objects, cat_id)
        if obj:
            i['category_name'] = obj.name
            i['category_last_name'] = obj.name
            i['category_number'] = getattr(obj, 'category_number', '')
            root_cat = obj

    elif level == 'level-2':
        l1_obj = get_category_by_id(level_one_category.objects, cat_id)
        if l1_obj:
            i['category_name'] = l1_obj.name
            i['category_last_name'] = l1_obj.name
            i['category_number'] = getattr(l1_obj, 'category_number', '')
            root_cat = DatabaseModel.get_document(category.objects, {'level_one_category_list__in': [cat_id]})
            if root_cat:
                breadcrumb.append(root_cat.name)

    elif level == 'level-3':
        l2_obj = get_category_by_id(level_two_category.objects, cat_id)
        if l2_obj:
            i['category_name'] = l2_obj.name
            i['category_last_name'] = l2_obj.name
            i['category_number'] = getattr(l2_obj, 'category_number', '')
            l1_obj = DatabaseModel.get_document(level_one_category.objects, {'level_two_category_list__in': [cat_id]})
            if l1_obj:
                root_cat = DatabaseModel.get_document(category.objects, {'level_one_category_list__in': [l1_obj.id]})
                if root_cat:
                    breadcrumb.append(root_cat.name)
                breadcrumb.append(l1_obj.name)

    elif level in ('level-4', 'level-5', 'level-6'):  # Handle level-4,5,6 together
        # Get current level object
        level_models = {
            'level-4': level_three_category,
            'level-5': level_four_category,
            'level-6': level_five_category,
        }
        current_model = level_models.get(level.replace('level-', 'level-').split('-')[1] >= '4' and level)
        if not current_model:
            current_model = { '4': level_three_category, '5': level_four_category, '6': level_five_category }[level.split('-')[1]]

        current_obj = get_category_by_id(current_model.objects, cat_id)
        if not current_obj:
            i['category_name'] = 'Unknown'
            i['category_last_name'] = 'Unknown'
            return

        i['category_name'] = current_obj.name
        i['category_last_name'] = current_obj.name
        i['category_number'] = getattr(current_obj, 'category_number', '')

        # Walk up the hierarchy
        parent_map = {
            'level-6': ('level_five_category_list', level_five_category),
            'level-5': ('level_four_category_list', level_four_category),
            'level-4': ('level_three_category_list', level_three_category),
        }

        current_id = cat_id
        current_level = level

        while current_level in ('level-4', 'level-5', 'level-6'):
            field_name, parent_model = parent_map[current_level]
            parent_obj = DatabaseModel.get_document(parent_model.objects, {f'{field_name}__in': [current_id]})
            if not parent_obj:
                break
            breadcrumb.append(parent_obj.name)
            current_id = parent_obj.id

            # Move up one level
            next_level_num = int(current_level.split('-')[1]) - 1
            if next_level_num < 3:
                break
            current_level = f'level-{next_level_num}'

        # Now get level-2 → level-1 → root
        if current_id:
            l2_obj = DatabaseModel.get_document(level_two_category.objects, {'level_three_category_list__in': [current_id]})
            if l2_obj:
                l1_obj = DatabaseModel.get_document(level_one_category.objects, {'level_two_category_list__in': [l2_obj.id]})
                if l1_obj:
                    root_cat = DatabaseModel.get_document(category.objects, {'level_one_category_list__in': [l1_obj.id]})
                    if root_cat:
                        breadcrumb.append(root_cat.name)
                    breadcrumb.append(l1_obj.name)
                breadcrumb.append(l2_obj.name)

    # Final assembly: Root > L1 > L2 > ... > Current
    if root_cat:
        breadcrumb.append(root_cat.name)

    # Reverse because we built from bottom-up or mixed
    breadcrumb.reverse()

    if breadcrumb:
        full_name = " > ".join(breadcrumb + [i['category_name']])
        i['category_name'] = full_name
    else:
        i['category_name'] = i['category_name']  # fallback

    # Ensure these fields always exist
    i.setdefault('category_last_name', i['category_name'].split(' > ')[-1] if ' > ' in i['category_name'] else i['category_name'])
    i.setdefault('category_number', '')