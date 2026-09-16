from django.contrib import admin

from .models import Result


@admin.register(Result)
class ResultAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "device_id",
        "mode",
        "event_type",
        "timestamp",
        "probability",
        "threat_detected_log",
        "threat_detected",
        "created_at",
    )

    search_fields = (
        "device_id",
    )

    list_filter = (
        "mode",
        "event_type",
        "threat_detected_log",
        "threat_detected",
        "created_at",
    )

    ordering = ("-created_at",)
