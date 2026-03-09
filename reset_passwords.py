import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'medical_billing.settings')
django.setup()

from workflow.models import User

users = User.objects.all()
for u in users:
    u.set_password('admin123')
    u.save()
    print(f'Password reset for {u.username}')
