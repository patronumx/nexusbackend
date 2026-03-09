import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'medical_billing.settings')
django.setup()

from workflow.models import User

# List of users to ensure exist with the corporate password
users_to_create = [
    {'username': 'admin', 'role': 'admin', 'type': 'admin'},
    {'username': 'employee_billing', 'role': 'billing', 'type': 'employee'},
    {'username': 'employee_payment', 'role': 'payment', 'type': 'employee'},
    {'username': 'employee_ar', 'role': 'ar_denial', 'type': 'employee'},
    {'username': 'employee_ops', 'role': 'operations_manager', 'type': 'employee'},
]

password = 'Staff_Access_2026!'

for u_data in users_to_create:
    user, created = User.objects.get_or_create(username=u_data['username'])
    user.set_password(password)
    user.role = u_data['role']
    user.user_type = u_data['type']
    if u_data['type'] == 'admin':
        user.is_staff = True
        user.is_superuser = True
    user.save()
    status = "Created" if created else "Updated"
    print(f"{status} user: {u_data['username']} with password: {password}")

print("Seeding complete.")
