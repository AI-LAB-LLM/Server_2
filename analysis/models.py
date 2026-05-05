from django.db import models


class Result(models.Model):
    class Mode(models.TextChoices):
        THREAT = "THREAT", "상시보고"
        PERIODIC = "PERIODIC", "주기보고"

    class EventType(models.TextChoices):
        PPG = "PPG", "PPG"
        IMU = "IMU", "IMU"

    class RiskLevel(models.TextChoices):
        LOW = "LOW", "하"
        MIDDLE = "MIDDLE", "중"
        HIGH = "HIGH", "상"

    device_id = models.CharField(
        max_length=100,
        db_index=True,
        help_text="전용 워치 device_id",
    )

    mode = models.CharField(
        max_length=20,
        choices=Mode.choices,
        help_text="THREAT=상시보고, PERIODIC=주기보고",
    )

    event_type = models.CharField(
        max_length=10,
        choices=EventType.choices,
        help_text="PPG 또는 IMU",
    )

    timestamp = models.BigIntegerField(
        help_text="UNIX timestamp milliseconds. 분석 결과가 대표하는 기준 시각",
    )

    probability = models.FloatField(
        null=True,
        blank=True,
        help_text="위험 확률값. 결과가 없으면 null",
    )

    risk_level = models.CharField(
        max_length=10,
        choices=RiskLevel.choices,
        null=True,
        blank=True,
        help_text="LOW/MIDDLE/HIGH. 결과가 없으면 null",
    )

    risk_detected = models.BooleanField(
        null=True,
        blank=True,
        help_text="위험 감지 여부. 결과가 없으면 null",
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "analysis_result"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["device_id", "timestamp"]),
            models.Index(fields=["mode"]),
            models.Index(fields=["event_type"]),
            models.Index(fields=["risk_detected"]),
        ]

    def __str__(self):
        return f"{self.device_id} / {self.mode} / {self.event_type} / {self.risk_level}"