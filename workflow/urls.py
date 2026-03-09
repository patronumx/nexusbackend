from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import JobViewSet, UserViewSet, JobHistoryViewSet

router = DefaultRouter()
router.register(r'jobs', JobViewSet)
router.register(r'users', UserViewSet)
router.register(r'history', JobHistoryViewSet, basename='history')

urlpatterns = [
    path('', include(router.urls)),
]
