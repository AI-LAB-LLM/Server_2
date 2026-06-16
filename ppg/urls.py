from django.urls import path
from . import views

app_name = "ppg"

urlpatterns = [
    # ... 기존 url들 ...
    path("device/<str:device_id>/", views.DeviceDashboardView.as_view(), name="device_dashboard"),
    path("api/apnea/process/",  views.ProcessWindowView.as_view(),  name="apnea_process"),
    path("api/apnea/baseline/", views.BaselineStartView.as_view(),  name="apnea_baseline"),
    path("api/apnea/records/",  views.ApneaRecordsView.as_view(),   name="apnea_records"),
    path("api/apnea/status/",   views.ModelStatusView.as_view(),    name="apnea_status"),
]