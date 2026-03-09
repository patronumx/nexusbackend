import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'medical_billing.settings')
django.setup()

from workflow.models import User

# Try to get existing admin or create a new one
username = 'admin'
password = 'admin123'

user, created = User.objects.get_or_create(username=username)
user.set_password(password)
user.role = 'admin'
user.is_staff = True
user.is_superuser = True
user.save()

print(f"User '{username}' updated/created with password '{password}'")
