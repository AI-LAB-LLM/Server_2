from django.urls import path
from .views import GeoDataIngestView, GeoTrackDataView, GeoTripMapView, GeoTripHistoryView, GeoTripCalendarView

urlpatterns = [
    path("data", GeoDataIngestView.as_view(), name="geo-data"),
    path("track", GeoTrackDataView.as_view(), name="geo-track"),
    path("trip_map/", GeoTripMapView.as_view(), name="geo-trip-map"),
    path("trip_history/", GeoTripHistoryView.as_view(), name="geo-trip-history"),
    path("trip_calendar/", GeoTripCalendarView.as_view(), name="geo-trip-calendar"),
]