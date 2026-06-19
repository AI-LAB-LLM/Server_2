from django.db import models
from monitoring.models import Protectee


class GeoData(models.Model):
    protectee = models.ForeignKey(
        Protectee,
        on_delete=models.CASCADE,
        related_name="geo_data"
    )
    device_id = models.CharField(max_length=100)
    timestamp = models.DateTimeField()
    pos_success = models.BooleanField()
    longitude = models.FloatField(null=True, blank=True)
    latitude = models.FloatField(null=True, blank=True)
    accuracy_h = models.FloatField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "geo_data"
        indexes = [
            models.Index(fields=["device_id", "timestamp"]),
            models.Index(fields=["protectee", "timestamp"]),
            models.Index(fields=["pos_success"]),
        ]
        ordering = ["timestamp"]

    def __str__(self):
        return f"{self.device_id} - {self.timestamp} - success={self.pos_success}"


class GeoProcessedData(models.Model):
    protectee = models.ForeignKey(
        Protectee,
        on_delete=models.CASCADE,
        related_name="geo_processed_data",
    )
    device_id = models.CharField(max_length=100)
    timestamp = models.DateTimeField()

    raw_latitude = models.FloatField(null=True, blank=True)
    raw_longitude = models.FloatField(null=True, blank=True)

    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)

    pos_success = models.BooleanField(default=True)

    gps_quality = models.CharField(max_length=50, null=True, blank=True)
    gps_filter_decision = models.CharField(max_length=100, null=True, blank=True)
    use_raw_for_gpr = models.BooleanField(null=True, blank=True)
    interp_method = models.CharField(max_length=100, null=True, blank=True)

    predicted_latitude = models.FloatField(null=True, blank=True)
    predicted_longitude = models.FloatField(null=True, blank=True)
    predicted_uncertainty_m = models.FloatField(null=True, blank=True)
    predicted_confidence_level = models.CharField(max_length=50, null=True, blank=True)

    state_primary = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "geo_processed_data"
        ordering = ["timestamp"]
        indexes = [
            models.Index(fields=["device_id", "timestamp"], name="geo_process_device__6b366b_idx"),
            models.Index(fields=["protectee", "timestamp"], name="geo_process_protect_2e5a10_idx"),
            models.Index(fields=["state_primary"], name="geo_process_state_p_7d1cde_idx"),
            models.Index(fields=["pos_success"], name="geo_process_pos_suc_6fdef2_idx"),
        ]

    def __str__(self):
        return f"{self.device_id} - {self.timestamp} - {self.state_primary}"


class GeoTripAnomalyResult(models.Model):
    class RouteLabel(models.TextChoices):
        KNOWN_NORMAL = "known_normal", "기존 정상 경로"
        UNSEEN_PATH_SAME_OD = "unseen_path_same_od", "같은 OD의 새로운 경로"
        ANOMALY = "anomaly", "이상 경로"

    protectee = models.ForeignKey(
        Protectee,
        on_delete=models.CASCADE,
        related_name="geo_trip_anomaly_results",
    )
    device_id = models.CharField(max_length=100)
    trip_start_time = models.DateTimeField()
    trip_end_time = models.DateTimeField()

    final_route_label = models.CharField(max_length=50, choices=RouteLabel.choices)
    od_key = models.CharField(max_length=100, null=True, blank=True)
    dtw_score = models.FloatField(null=True, blank=True)
    threshold = models.FloatField(null=True, blank=True)
    message = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "geo_trip_anomaly_result"
        ordering = ["trip_start_time"]
        indexes = [
            models.Index(fields=["device_id", "trip_start_time"], name="geo_trip_an_device__e847f7_idx"),
            models.Index(fields=["protectee", "trip_start_time"], name="geo_trip_an_protect_8892b4_idx"),
            models.Index(fields=["final_route_label"], name="geo_trip_an_final_r_ccfd78_idx"),
        ]

    def __str__(self):
        return f"{self.device_id} - {self.trip_start_time} ~ {self.trip_end_time} - {self.final_route_label}"