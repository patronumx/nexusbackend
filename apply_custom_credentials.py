import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'medical_billing.settings')
django.setup()

from workflow.models import User

# Exact credentials requested by the USER
user_manifest = [
    {'username': 'admin', 'password': 'Admin123!', 'role': 'admin', 'type': 'admin'},
    {'username': 'manager', 'password': 'Manager123!', 'role': 'operations_manager', 'type': 'employee'},
    {'username': 'billing_user', 'password': 'Billing123!', 'role': 'billing', 'type': 'employee'},
    {'username': 'payment_user', 'password': 'Payment123!', 'role': 'payment', 'type': 'employee'},
    {'username': 'ar_user', 'password': 'ARDenial2026!', 'role': 'ar_denial', 'type': 'employee'},
]

print("💉 Applying Specialized Identity Protocol...")

for data in user_manifest:
    user, created = User.objects.get_or_create(username=data['username'])
    user.set_password(data['password'])
    user.role = data['role']
    user.user_type = data['type']
    
    if data['type'] == 'admin':
        user.is_staff = True
        user.is_superuser = True
    
    user.save()
    action = "PROVISIONED" if created else "REALIGNED"
    print(f"✅ {action} Identity: [{data['username']}] | ROLE: {data['role']}")

print("\n✨ Identity Matrix Synchronized.")
