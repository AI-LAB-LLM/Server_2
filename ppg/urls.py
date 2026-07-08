from django.urls import path
from . import views

app_name = "ppg"

urlpatterns = [
    # 대시보드
    path("device/<str:device_id>/",        views.DeviceDashboardView.as_view(),       name="device_dashboard"),

    # Apnea API
    path("api/apnea/records/",  views.ApneaRecordsView.as_view(),  name="apnea_records"),
    path("api/apnea/status/",   views.ModelStatusView.as_view(),   name="apnea_status"),

    # Threat API
    path("api/event_status/", views.EventStatusView.as_view(), name="event_status"),
]