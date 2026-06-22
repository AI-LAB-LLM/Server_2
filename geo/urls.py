from django.urls import path
from .views import GeoDataIngestView, GeoTrackDataView

urlpatterns = [
    path("data", GeoDataIngestView.as_view(), name="geo-data"),
    path("track", GeoTrackDataView.as_view(), name="geo-track"),
]