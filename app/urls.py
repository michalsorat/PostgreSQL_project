from django.urls import path
from . import views
from . import views_ORM

urlpatterns = [
    path('v1/health/', views.uptime),
    path('v1/ov/submissions/', views.check_method),
    path('v1/ov/submissions/<int:id>/', views.delete_submissions),
    path('v1/companies/', views.get_companies),
    path('v2/ov/submissions/', views_ORM.check_method_ORM),
    path('v2/ov/submissions/<int:id>', views_ORM.check_method_ORM),
    path('v2/ov/submissions/<int:id>/', views_ORM.check_method_ORM)
]