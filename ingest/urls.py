from django.urls import path
from .views import ThreatIngestView

urlpatterns = [
    path("threat/ingest", ThreatIngestView.as_view(), name="threat-ingest"),
]
