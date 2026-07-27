from django.contrib import admin
from .models import GeoData, GeoProcessedData, GeoTripAnomalyResult


@admin.register(GeoData)
class GeoDataAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "device_id",
        "protectee",
        "timestamp",
        "pos_success",
        "latitude",
        "longitude",
        "accuracy_h",
    )
    list_filter = ("pos_success",)
    search_fields = ("device_id", "protectee__name", "protectee__device_id")
    ordering = ("-timestamp",)


@admin.register(GeoProcessedData)
class GeoProcessedDataAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "device_id",
        "timestamp",
        "pos_success",
        "raw_latitude",
        "raw_longitude",
        "latitude",
        "longitude",
        "gps_quality",
        "gps_filter_decision",
        "state_primary",
    )
    list_filter = ("pos_success", "gps_quality", "gps_filter_decision", "state_primary")
    search_fields = ("device_id", "protectee__name", "protectee__device_id")
    ordering = ("-timestamp",)
    readonly_fields = (
        "created_at",
        "updated_at",
    )


@admin.register(GeoTripAnomalyResult)
class GeoTripAnomalyResultAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "device_id",
        "trip_start_time",
        "trip_end_time",
        "final_route_label",
        "od_key",
        "dtw_score",
        "threshold",
        "created_at",
    )
    list_filter = ("final_route_label",)
    search_fields = ("device_id", "protectee__name", "protectee__device_id", "od_key")
    ordering = ("-trip_start_time",)
    readonly_fields = ("created_at",)