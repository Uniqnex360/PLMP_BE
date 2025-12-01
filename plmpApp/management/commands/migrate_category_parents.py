from django.core.management.base import BaseCommand
from plmpApp.models import (
    category,
    level_one_category,
    level_two_category,
    level_three_category,
    level_four_category,
    level_five_category
)

class Command(BaseCommand):
    help = 'Migrate parents (ListField) for all category levels'

    def handle(self, *args, **options):
        self.stdout.write("Starting migration for multiple parents...")
        
        updated_count = 0
        
        # ---------------------------------------------------------
        # Level 1: category -> level_one_category
        # ---------------------------------------------------------
        self.stdout.write("Processing level_one_category...")
        for cat in category.objects():
            for level_one_ref in cat.level_one_category_list:
                try:
                    level_one_obj = level_one_category.objects(id=level_one_ref.id).first()
                    if level_one_obj:
                        # Check if this parent is already in the list
                        if cat not in level_one_obj.parents:
                            level_one_obj.parents.append(cat)
                            level_one_obj.save()
                            updated_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error L1: {e}"))
        
        # ---------------------------------------------------------
        # Level 2: level_one -> level_two
        # ---------------------------------------------------------
        self.stdout.write("Processing level_two_category...")
        for level_one_obj in level_one_category.objects():
            for level_two_ref in level_one_obj.level_two_category_list:
                try:
                    level_two_obj = level_two_category.objects(id=level_two_ref.id).first()
                    if level_two_obj:
                        if level_one_obj not in level_two_obj.parents:
                            level_two_obj.parents.append(level_one_obj)
                            level_two_obj.save()
                            updated_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error L2: {e}"))
        
        # ---------------------------------------------------------
        # Level 3: level_two -> level_three
        # ---------------------------------------------------------
        self.stdout.write("Processing level_three_category...")
        for level_two_obj in level_two_category.objects():
            for level_three_ref in level_two_obj.level_three_category_list:
                try:
                    level_three_obj = level_three_category.objects(id=level_three_ref.id).first()
                    if level_three_obj:
                        if level_two_obj not in level_three_obj.parents:
                            level_three_obj.parents.append(level_two_obj)
                            level_three_obj.save()
                            updated_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error L3: {e}"))
        
        # ---------------------------------------------------------
        # Level 4: level_three -> level_four
        # ---------------------------------------------------------
        self.stdout.write("Processing level_four_category...")
        for level_three_obj in level_three_category.objects():
            for level_four_ref in level_three_obj.level_four_category_list:
                try:
                    level_four_obj = level_four_category.objects(id=level_four_ref.id).first()
                    if level_four_obj:
                        if level_three_obj not in level_four_obj.parents:
                            level_four_obj.parents.append(level_three_obj)
                            level_four_obj.save()
                            updated_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error L4: {e}"))
        
        # ---------------------------------------------------------
        # Level 5: level_four -> level_five
        # ---------------------------------------------------------
        self.stdout.write("Processing level_five_category...")
        for level_four_obj in level_four_category.objects():
            for level_five_ref in level_four_obj.level_five_category_list:
                try:
                    level_five_obj = level_five_category.objects(id=level_five_ref.id).first()
                    if level_five_obj:
                        if level_four_obj not in level_five_obj.parents:
                            level_five_obj.parents.append(level_four_obj)
                            level_five_obj.save()
                            updated_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error L5: {e}"))
        
        self.stdout.write(self.style.SUCCESS(f"Migration complete! Updated/Linked {updated_count} parent relationships."))