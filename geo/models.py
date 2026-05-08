from django.db import models

from monitoring.models import Protectee


class GeoData(models.Model):
    protectee = models.ForeignKey(
        Protectee,
        on_delete=models.CASCADE,
        related_name="geo_data",
        help_text="device_id로 매핑된 보호 대상자",
    )

    device_id = models.CharField(
        max_length=100,
        db_index=True,
        help_text="중앙서버에서 전달받은 전용 워치 device_id",
    )

    timestamp = models.DateTimeField(
        db_index=True,
        help_text="GPS 측정 시간. API에서는 UNIX ms로 받고, DB에는 UTC DateTime으로 저장",
    )

    latitude = models.FloatField(
        help_text="위도",
    )

    longitude = models.FloatField(
        help_text="경도",
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
        help_text="성신서버 DB 저장 시간",
    )

    class Meta:
        db_table = "geo_data"
        ordering = ["-timestamp"]
        indexes = [
            models.Index(fields=["device_id", "timestamp"]),
            models.Index(fields=["protectee", "timestamp"]),
        ]

    def __str__(self):
        return f"{self.device_id} / {self.timestamp} / {self.latitude}, {self.longitude}"