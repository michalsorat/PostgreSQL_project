"""
Definition of urls for dbs_sorat_web_app.
"""

from datetime import datetime
from django.urls import path, include
from django.contrib import admin
from django.contrib.auth.views import LoginView, LogoutView


urlpatterns = [
    path('', include('app.urls'))
]
