from rest_framework import serializers
from .models import User, Job, JobHistory, TimeTracking

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'user_type', 'role', 'full_name', 'is_active']
        read_only_fields = ['role', 'is_active']  # Users can't change their own role/status via standard update

class JobHistorySerializer(serializers.ModelSerializer):
    user_name = serializers.CharField(source='user.username', read_only=True)
    
    class Meta:
        model = JobHistory
        fields = '__all__'

class TimeTrackingSerializer(serializers.ModelSerializer):
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    
    class Meta:
        model = TimeTracking
        fields = '__all__'

class JobSerializer(serializers.ModelSerializer):
    created_by_name = serializers.CharField(source='created_by.username', read_only=True)
    assigned_to_name = serializers.CharField(source='assigned_to.username', read_only=True)
    history = JobHistorySerializer(many=True, read_only=True)
    time_tracks = TimeTrackingSerializer(many=True, read_only=True)

    class Meta:
        model = Job
        fields = '__all__'
        read_only_fields = ['created_by', 'current_role', 'created_at', 'updated_at']

class BulkUploadSerializer(serializers.Serializer):
    file = serializers.FileField()

class ChangePasswordSerializer(serializers.Serializer):
    old_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True)

    def validate_new_password(self, value):
        if len(value) < 8:
            raise serializers.ValidationError("Password must be at least 8 characters long.")
        if not any(char.isupper() for char in value):
            raise serializers.ValidationError("Password must contain at least one uppercase letter.")
        if not any(char.islower() for char in value):
            raise serializers.ValidationError("Password must contain at least one lowercase letter.")
        if not any(char.isdigit() for char in value):
            raise serializers.ValidationError("Password must contain at least one digit.")
        if not any(char in '!@?><%^&*()-_+=' for char in value):
            raise serializers.ValidationError("Password must contain at least one special character.")
        return value

