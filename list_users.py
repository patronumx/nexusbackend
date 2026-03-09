import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'medical_billing.settings')
django.setup()

from workflow.models import User

print("Existing Users in System:")
print("-" * 50)
for u in User.objects.all():
    print(f"Username: {u.username:<20} | Role: {u.role:<20} | Type: {u.user_type}")
print("-" * 50)
