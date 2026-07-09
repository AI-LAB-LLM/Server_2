from django.contrib import admin
from .models import Protectee, MonitoringSession, SensorWindow


@admin.register(Protectee)
class ProtecteeAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "device_id",
        "name",
        "gender",
        "created_at",
        "updated_at",
    )
    search_fields = ("device_id", "name")
    list_filter = ("gender",)
    ordering = ("id",)


@admin.register(MonitoringSession)
class MonitoringSessionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "get_device_id",
        "mode",
        "get_is_active",
        "started_at",
        "ended_at",
    )

    search_fields = (
        "protectee__device_id",
        "protectee__name",
    )

    list_filter = (
        "mode",
        "started_at",
        "ended_at",
    )

    ordering = ("-started_at",)

    def get_device_id(self, obj):
        return obj.protectee.device_id

    def get_is_active(self, obj):
        return obj.ended_at is None

    get_device_id.short_description = "device_id"
    get_is_active.short_description = "is_active"
    get_is_active.boolean = True


@admin.register(SensorWindow)
class SensorWindowAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "session",
        "protectee_id",
        "get_device_id",
        "get_mode",
        "started_at",
        "ended_at",
        "created_at",
    )

    search_fields = (
        "session__protectee__device_id",
        "session__protectee__name",
    )

    list_filter = (
        "session__mode",
        "created_at",
    )

    ordering = ("-started_at",)

    def get_device_id(self, obj):
        return obj.session.protectee.device_id

    def get_mode(self, obj):
        return obj.session.mode

    get_device_id.short_description = "device_id"
    get_mode.short_description = "mode"