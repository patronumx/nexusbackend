import pandas as pd
from rest_framework import viewsets, permissions, status, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from django.db import transaction
from django_filters.rest_framework import DjangoFilterBackend
from .models import User, Job, JobHistory
from .serializers import UserSerializer, JobSerializer, JobHistorySerializer, BulkUploadSerializer, ChangePasswordSerializer

class JobViewSet(viewsets.ModelViewSet):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    permission_classes = [permissions.IsAuthenticated]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['status', 'priority', 'current_role']
    search_fields = ['patient_name', 'claim_id', 'patient_id', 'insurance_provider']
    ordering_fields = ['created_at', 'priority', 'date_of_service']

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    def get_queryset(self):
        user = self.request.user
        queryset = super().get_queryset()
        
        # Admin and Ops Manager see everything (Control Room Visibility)
        if user.role in ['admin', 'operations_manager']:
            return queryset
            
        # Operational Users (Billing, Payment, AR) pull ONLY from their specific role queue.
        # This prevents manual assignment bias and enforces the queue-based system.
        return queryset.filter(current_role=user.role)

    def _check_terminal(self, job):
        if job.status in ['closed_paid', 'closed_adjusted', 'written_off', 'dismissed']:
            from rest_framework.exceptions import ValidationError
            raise ValidationError("Operational Lockdown: Terminal nodes are read-only artifacts.")

    @action(detail=True, methods=['post'])
    def submit(self, request, pk=None):
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only submit if draft or rejected
        if job.status not in ['draft', 'rejected']:
            return Response(
                {"error": "Only draft or rejected jobs can be submitted"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
            
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        # Accurate Time Tracking: Close previous stage, start next
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # DIRECT ROUTING: Billing → Payment (bypass Clearinghouse)
        TimeTracking.objects.create(job=job, status='accepted')
            
        # Transition logic
        old_status = job.status
        job.status = 'accepted'
        job.current_role = 'payment'  # Direct to Payment Queue
        job.save()
        
        # Audit Log
        duration = (now - job.created_at).total_seconds() if last_track else 0
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Submitted Claim (Direct to Payment)",
            from_status=old_status,
            to_status='accepted',
            notes=f"Submitted by billing staff. Routed directly to Payment queue. Time in billing: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def accept(self, request, pk=None):
        """Accept a claim (Clearinghouse Review) - moves to Payment queue"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only accept if submitted or under_review
        if job.status not in ['submitted', 'under_review']:
            return Response(
                {"error": "Only submitted or under_review jobs can be accepted"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Create new track for 'accepted'
        TimeTracking.objects.create(job=job, status='accepted')
        
        # Transition logic
        old_status = job.status
        job.status = 'accepted'
        job.current_role = 'payment'  # Auto-moves to Payment queue
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Clearinghouse Accepted",
            from_status=old_status,
            to_status='accepted',
            notes=f"Accepted by clearinghouse. Time in review: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def reject(self, request, pk=None):
        """Reject a claim (Clearinghouse Review) - returns to Billing queue"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only reject if submitted or under_review
        if job.status not in ['submitted', 'under_review']:
            return Response(
                {"error": "Only submitted or under_review jobs can be rejected"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get rejection reason from request
        reason = request.data.get('reason', 'No reason provided')
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        # Close previous track
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Create new track for 'rejected' (Billing correction)
        TimeTracking.objects.create(job=job, status='rejected')
        
        # Transition logic
        old_status = job.status
        job.status = 'rejected'
        job.current_role = 'billing'  # Returns to Billing queue for correction
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Clearinghouse Rejected",
            from_status=old_status,
            to_status='rejected',
            notes=f"Rejected by clearinghouse. Reason: {reason}. Time in review: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def post_payment_full(self, request, pk=None):
        """Post full payment - closes the job"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only post payment if accepted
        if job.status != 'accepted':
            return Response(
                {"error": "Only accepted jobs can have payments posted"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get payment amount from request
        payment_amount = request.data.get('payment_amount')
        if not payment_amount:
            return Response(
                {"error": "payment_amount is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Terminal status
        
        # Transition logic
        old_status = job.status
        job.status = 'paid_full'
        job.payment_amount = payment_amount
        job.current_role = 'archive'  # Job is complete
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Payment Posted - Full",
            from_status=old_status,
            to_status='paid_full',
            notes=f"Full payment posted: ${payment_amount}. Time in payment queue: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def post_payment_partial(self, request, pk=None):
        """Post partial payment - sends to AR queue"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only post payment if accepted
        if job.status != 'accepted':
            return Response(
                {"error": "Only accepted jobs can have payments posted"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get payment amount from request
        payment_amount = request.data.get('payment_amount')
        if not payment_amount:
            return Response(
                {"error": "payment_amount is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # AR stage
        TimeTracking.objects.create(job=job, status='paid_partial')
        
        # Transition logic
        old_status = job.status
        job.status = 'paid_partial'
        job.payment_amount = payment_amount
        job.current_role = 'ar_denial'  # Moves to AR queue for follow-up
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Payment Posted - Partial",
            from_status=old_status,
            to_status='paid_partial',
            notes=f"Partial payment posted: ${payment_amount} of ${job.claim_amount}. Balance: ${float(job.claim_amount) - float(payment_amount)}. Time in payment queue: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def post_payment_denied(self, request, pk=None):
        """Mark payment as denied - sends to Denial Management queue"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only deny if accepted
        if job.status != 'accepted':
            return Response(
                {"error": "Only accepted jobs can be marked as denied"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get denial reason from request
        denial_reason = request.data.get('reason', 'No reason provided')
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Denial stage
        TimeTracking.objects.create(job=job, status='denied')
        
        # Transition logic
        old_status = job.status
        job.status = 'denied'
        job.current_role = 'ar_denial'  # Moves to Denial Management queue
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Payment Denied",
            from_status=old_status,
            to_status='denied',
            notes=f"Payment denied. Reason: {denial_reason}. Time in payment queue: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def resubmit_claim(self, request, pk=None):
        """Resubmit a denied or partial payment claim"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only resubmit if denied or paid_partial
        if job.status not in ['denied', 'paid_partial']:
            return Response(
                {"error": "Only denied or partial payment jobs can be resubmitted"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get resubmission notes
        notes = request.data.get('notes', 'Resubmitted for review')
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Back to 'submitted' (Clearinghouse review)
        TimeTracking.objects.create(job=job, status='submitted')
        
        # Transition logic
        old_status = job.status
        job.status = 'submitted'  # Back to clearinghouse for review
        job.current_role = 'operations_manager'
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Claim Resubmitted",
            from_status=old_status,
            to_status='submitted',
            notes=f"Resubmitted from AR. {notes}. Time in AR queue: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def write_off(self, request, pk=None):
        """Write off uncollectable balance"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Validation: Can only write off if denied or paid_partial
        if job.status not in ['denied', 'paid_partial']:
            return Response(
                {"error": "Only denied or partial payment jobs can be written off"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get write-off reason and amount
        reason = request.data.get('reason', 'Uncollectable')
        write_off_amount = request.data.get('write_off_amount')
        
        if not write_off_amount:
            return Response(
                {"error": "write_off_amount is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Terminal status
        
        # Transition logic
        old_status = job.status
        job.status = 'written_off'
        job.current_role = 'archive'  # Job is closed
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Balance Written Off",
            from_status=old_status,
            to_status='written_off',
            notes=f"Written off ${write_off_amount}. Reason: {reason}. Time in AR queue: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def close_job(self, request, pk=None):
        """Close a job manually (admin only)"""
        job = self.get_object()
        self._check_terminal(job)
        
        # Get closure reason
        reason = request.data.get('reason', 'Manually closed')
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        # Terminal status
        
        # Transition logic
        old_status = job.status
        job.status = 'closed'
        job.current_role = 'archive'
        job.save()
        
        # Audit Log
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Job Closed",
            from_status=old_status,
            to_status='closed',
            notes=f"Manually closed. Reason: {reason}. Total time: {int(duration/60)} mins"
        )
        
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def hold(self, request, pk=None):
        """Put job on hold with mandatory reason"""
        job = self.get_object()
        self._check_terminal(job)
        reason = request.data.get('reason')
        if not reason:
            return Response({"error": "reason is required to put job on hold"}, status=status.HTTP_400_BAD_REQUEST)
            
        old_status = job.status
        job.status = 'on_hold'
        job.save()
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        TimeTracking.objects.create(job=job, status='on_hold')
        
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Placed On Hold",
            from_status=old_status,
            to_status='on_hold',
            notes=reason
        )
        return Response(JobSerializer(job).data)

    @action(detail=True, methods=['post'])
    def escalate(self, request, pk=None):
        """Escalate job to Operations Manager"""
        job = self.get_object()
        self._check_terminal(job)
        reason = request.data.get('reason')
        if not reason:
            return Response({"error": "reason is required for escalation"}, status=status.HTTP_400_BAD_REQUEST)
            
        old_status = job.status
        job.status = 'escalated'
        job.save()
        
        from django.utils import timezone
        from .models import TimeTracking
        now = timezone.now()
        
        last_track = TimeTracking.objects.filter(job=job, exited_at__isnull=True).last()
        if last_track:
            last_track.exited_at = now
            last_track.duration_seconds = int((now - last_track.entered_at).total_seconds())
            last_track.save()
            
        TimeTracking.objects.create(job=job, status='escalated')
        
        JobHistory.objects.create(
            job=job,
            user=request.user,
            action="Escalated to Manager",
            from_status=old_status,
            to_status='escalated',
            notes=reason
        )
        return Response(JobSerializer(job).data)

    @action(detail=False, methods=['get'])
    def stuck_jobs(self, request):
        """Identify jobs that have exceeded SLA thresholds"""
        from django.utils import timezone
        from datetime import timedelta
        
        now = timezone.now()
        
        # Define SLA thresholds (in hours)
        sla_thresholds = {
            'draft': 24,           # 1 day
            'submitted': 48,       # 2 days
            'under_review': 72,    # 3 days
            'accepted': 48,        # 2 days
            'denied': 120,         # 5 days
            'paid_partial': 120,   # 5 days
            'on_hold': 168,        # 7 days
        }
        
        stuck_jobs = []
        
        for status, threshold_hours in sla_thresholds.items():
            threshold = timedelta(hours=threshold_hours)
            cutoff_time = now - threshold
            
            # Find jobs in this status longer than threshold
            jobs = Job.objects.filter(
                status=status,
                updated_at__lt=cutoff_time
            ).select_related('created_by', 'assigned_to')
            
            for job in jobs:
                time_stuck = now - job.updated_at
                days_stuck = time_stuck.days
                hours_stuck = int(time_stuck.total_seconds() / 3600)
                
                stuck_jobs.append({
                    'id': job.id,
                    'claim_id': job.claim_id,
                    'patient_name': job.patient_name,
                    'status': job.status,
                    'current_role': job.current_role,
                    'priority': job.priority,
                    'claim_amount': str(job.claim_amount),
                    'days_stuck': days_stuck,
                    'hours_stuck': hours_stuck,
                    'sla_threshold_hours': threshold_hours,
                    'updated_at': job.updated_at.isoformat(),
                    'severity': 'critical' if hours_stuck > threshold_hours * 2 else 'warning'
                })
        
        # Sort by days stuck (most urgent first)
        stuck_jobs.sort(key=lambda x: x['hours_stuck'], reverse=True)
        
        return Response({
            'count': len(stuck_jobs),
            'jobs': stuck_jobs
        })

    @action(detail=False, methods=['post'], parser_classes=[MultiPartParser, FormParser])
    def bulk_upload(self, request):
        """
        Handle Excel upload: One row = one job.
        Required columns: Claim ID, Patient Name, Payer, Priority, Amount, DOS
        """
        # Ensure only Admin/Ops can upload
        if request.user.role not in ['admin', 'operations_manager']:
            return Response({"error": "Permission denied"}, status=status.HTTP_403_FORBIDDEN)
            
        serializer = BulkUploadSerializer(data=request.data)
        if serializer.is_valid():
            file_obj = request.FILES['file']
            try:
                # Read Excel
                df = pd.read_excel(file_obj)
                
                # Check required columns
                required_cols = ['Claim ID', 'Patient Name', 'Payer', 'Priority', 'Amount', 'Patient ID']
                missing = [col for col in required_cols if col not in df.columns]
                if missing:
                    return Response({"error": f"Missing columns: {', '.join(missing)}"}, status=status.HTTP_400_BAD_REQUEST)
                
                # Identify extra columns for metadata capture
                extra_cols = [col for col in df.columns if col not in required_cols]
                
                created_count = 0
                errors = []
                
                with transaction.atomic():
                    for index, row in df.iterrows():
                        try:
                            # Capture tactical metadata
                            meta_data = {col: row[col] for col in extra_cols if pd.notna(row[col])}
                            
                            # Create Job with Metadata preservation
                            job = Job.objects.create(
                                claim_id=str(row['Claim ID']),
                                patient_name=row['Patient Name'],
                                patient_id=str(row['Patient ID']),
                                insurance_provider=row['Payer'],
                                priority=str(row['Priority']).lower(),
                                claim_amount=row['Amount'],
                                created_by=request.user,
                                status='draft',
                                metadata=meta_data
                            )
                            
                            # START FIRST TIME TRACK
                            from .models import TimeTracking
                            TimeTracking.objects.create(job=job, status='draft')
                            
                            # Create history entry (Task Generation)
                            JobHistory.objects.create(
                                job=job,
                                user=request.user,
                                action="Job Created (Bulk Upload)",
                                from_status="New",
                                to_status="draft",
                                notes=f"Imported from {file_obj.name}"
                            )
                            
                            created_count += 1
                        except Exception as e:
                            errors.append(f"Row {index+2}: {str(e)}")
                
                return Response({
                    "message": f"Successfully created {created_count} jobs.",
                    "warnings": errors
                }, status=status.HTTP_201_CREATED)
                
            except Exception as e:
                return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        # Handle 'my_profile' shortcut used by frontend
        if self.request.query_params.get('my_profile') == 'true':
            return User.objects.filter(id=self.request.user.id)
            
        role = self.request.user.role
        
        # Admin has absolute system-wide visibility
        if role == 'admin':
            return User.objects.all()
            
        # Ops Manager sees standard operational staff for supervision
        if role == 'operations_manager':
            return User.objects.filter(role__in=['billing', 'payment', 'ar_denial'])
            
        # Standard staff can ONLY see their own deployment node
        return User.objects.filter(id=self.request.user.id)

    def perform_create(self, serializer):
        # Mandatory Admin authorization for identity creation
        if self.request.user.role != 'admin':
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Identity provision restricted to Admin Executive.")
        serializer.save()

    def perform_update(self, serializer):
        # Only Admin or the User themselves can modify identity nodes
        if self.request.user.role != 'admin' and self.request.user.id != serializer.instance.id:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Identity modification restricted.")
        serializer.save()

    @action(detail=False, methods=['post'], url_path='change-password')
    def change_password(self, request):
        user = request.user
        serializer = ChangePasswordSerializer(data=request.data)

        if serializer.is_valid():
            if not user.check_password(serializer.data.get("old_password")):
                return Response({"old_password": ["Wrong password."]}, status=status.HTTP_400_BAD_REQUEST)
            
            user.set_password(serializer.data.get("new_password"))
            user.save()
            return Response({"status": "password set"}, status=status.HTTP_200_OK)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class JobHistoryViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = JobHistory.objects.all().order_by('-timestamp')
    serializer_class = JobHistorySerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        # Global audit logs restricted to Admin and Ops Manager
        if self.request.user.role not in ['admin', 'operations_manager']:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Audit manifest access restricted.")
        return super().get_queryset()
