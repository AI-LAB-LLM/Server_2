from django.urls import path
from .page_views import GeoDeviceMapPageView

app_name = "geo_web"

urlpatterns = [
    path("device/<str:device_id>/", GeoDeviceMapPageView.as_view(), name="device_map"),
]
