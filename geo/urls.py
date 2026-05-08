from django.urls import path

from .views import GeoDataCreateAPIView


urlpatterns = [
    path("data/", GeoDataCreateAPIView.as_view(), name="geo-data-create"),
]