from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils.translation import gettext_lazy as _

class User(AbstractUser):
    USER_TYPE_CHOICES = [
        ('admin', 'Admin'),
        ('employee', 'Employee'),
    ]
    
    ROLE_CHOICES = [
        ('admin', 'Administrator'),
        ('operations_manager', 'Operations Manager'),
        ('billing', 'Billing Staff'),
        ('payment', 'Payment Posting Staff'),
        ('ar_denial', 'AR / Denial Staff'),
    ]
    
    user_type = models.CharField(max_length=10, choices=USER_TYPE_CHOICES, default='employee')
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='billing')
    full_name = models.CharField(max_length=100, blank=True)
    created_by = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='created_users')

    def __str__(self):
        return f"{self.username} ({self.get_role_display()})"

class Job(models.Model):
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('submitted', 'Submitted'),
        ('under_review', 'Under Review'),
        ('accepted', 'Accepted'),
        ('rejected', 'Rejected'),
        ('paid_full', 'Paid in Full'),
        ('paid_partial', 'Paid Partial'),
        ('denied', 'Denied'),
        ('denial_management', 'Denial Management'),
        ('ar_followup', 'AR Follow-up'),
        ('closed_paid', 'Closed - Paid'),
        ('closed_adjusted', 'Closed - Adjusted'),
        ('on_hold', 'On Hold'),
        ('escalated', 'Escalated'),
        ('appeal_in_progress', 'Appeal in Progress'),
        ('written_off', 'Written Off'),
        ('dismissed', 'Dismissed'),
    ]
    
    PRIORITY_CHOICES = [
        ('normal', 'Normal'),
        ('high', 'High'),
        ('urgent', 'Urgent'),
    ]
    
    ROLE_STAGES = {
        'draft': 'billing',
        'submitted': 'clearinghouse',  # Handled by clearinghouse view
        'under_review': 'clearinghouse',
        'accepted': 'payment',
        'rejected': 'billing',
        'paid_full': 'system',
        'paid_partial': 'ar',
        'denied': 'denial',
        'denial_management': 'denial',
        'ar_followup': 'ar',
        'closed_paid': 'archive',
        'closed_adjusted': 'archive'
    }

    claim_id = models.CharField(max_length=50, unique=True, help_text="Unique Claim ID from source system")
    patient_name = models.CharField(max_length=100)
    patient_id = models.CharField(max_length=50) # Internal System ID
    date_of_service = models.DateField(null=True, blank=True)
    claim_amount = models.DecimalField(max_digits=10, decimal_places=2)
    payment_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    insurance_provider = models.CharField(max_length=100, help_text="Payer")
    priority = models.CharField(max_length=10, choices=PRIORITY_CHOICES, default='normal')
    description = models.TextField(blank=True, null=True, help_text="Operational Notes/Procedural Narrative")
    metadata = models.JSONField(default=dict, blank=True, help_text="Captured tactical metadata from Ingestion Bay")
    
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default='draft')
    current_role = models.CharField(max_length=20, default='billing')
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    created_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name='created_jobs')
    assigned_to = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='assigned_jobs')

    def save(self, *args, **kwargs):
        # Auto-update current_role based on status
        # Note: 'clearinghouse' queue is viewed by Ops Manager or specific logic, 
        # but for now we map loosely to main roles.
        # Adjusted slightly to match ROLES.md queues
        # Tactical Role Registry Mapping
        if self.status in ['draft', 'rejected']:
            self.current_role = 'billing'
        elif self.status in ['submitted', 'under_review', 'escalated']:
            self.current_role = 'operations_manager'
        elif self.status == 'accepted':
            self.current_role = 'payment'
        elif self.status in ['denied', 'denial_management', 'paid_partial', 'ar_followup', 'appeal_in_progress']:
            self.current_role = 'ar_denial'
        elif self.status in ['closed_paid', 'closed_adjusted', 'written_off', 'dismissed']:
            self.current_role = 'archive'
        elif self.status == 'on_hold':
            # On hold keeps current role for department-specific freeze
            pass
        
        # 'on_hold' trace persists in the current deployment node unless overridden above
        
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.patient_name} - {self.get_status_display()}"

class JobHistory(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='history')
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    action = models.CharField(max_length=50)
    from_status = models.CharField(max_length=30)
    to_status = models.CharField(max_length=30)
    timestamp = models.DateTimeField(auto_now_add=True)
    notes = models.TextField(blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)

    class Meta:
        ordering = ['-timestamp']

class TimeTracking(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='time_tracks')
    status = models.CharField(max_length=30)
    entered_at = models.DateTimeField(auto_now_add=True)
    exited_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return f"{self.job} - {self.status}"
