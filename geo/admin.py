from django.contrib import admin

from .models import GeoData


@admin.register(GeoData)
class GeoDataAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "protectee",
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "created_at",
    ]

    list_filter = [
        "created_at",
        "timestamp",
    ]

    search_fields = [
        "device_id",
        "protectee__device_id",
        "protectee__name",
    ]

    ordering = [
        "-timestamp",
    ]