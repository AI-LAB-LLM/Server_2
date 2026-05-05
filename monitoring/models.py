from datetime import timedelta
from django.db import models


class Protectee(models.Model):
    class Gender(models.TextChoices):
        MALE = "MALE", "남성"
        FEMALE = "FEMALE", "여성"
        UNKNOWN = "UNKNOWN", "미상"

    device_id = models.CharField(
        max_length=100,
        unique=True,
        db_index=True,
        help_text="전용 워치 device_id",
    )

    name = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="보호 대상자 이름",
    )

    gender = models.CharField(
        max_length=20,
        choices=Gender.choices,
        default=Gender.UNKNOWN,
        help_text="보호 대상자 성별",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "protectee"
        ordering = ["id"]

    def __str__(self):
        return f"{self.name or '이름 미등록'} / {self.device_id}"


class MonitoringSession(models.Model):
    class Mode(models.TextChoices):
        THREAT = "THREAT", "이벤트보고"
        PERIODIC = "PERIODIC", "주기보고"
        CALIBRATION = "CALIBRATION", "캘리브레이션"

    protectee = models.ForeignKey(
        Protectee,
        on_delete=models.CASCADE,
        related_name="monitoring_sessions",
    )

    mode = models.CharField(
        max_length=20,
        choices=Mode.choices,
        help_text="THREAT=상시보고, PERIODIC=주기보고, CALIBRATION=캘리브레이션",
    )

    started_at = models.DateTimeField(auto_now_add=True)

    ended_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="세션 종료 시각. null이면 진행 중",
    )

    class Meta:
        db_table = "monitoring_session"
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["mode", "ended_at"]),
            models.Index(fields=["protectee", "started_at"]),
        ]

    def __str__(self):
        return f"{self.protectee.device_id} / {self.mode}"


class SensorWindow(models.Model):
    session = models.ForeignKey(
        MonitoringSession,
        on_delete=models.CASCADE,
        related_name="sensor_windows",
    )

    sample_rate_hz = models.PositiveIntegerField(default=25)
    duration_sec = models.PositiveIntegerField(default=12)

    started_at = models.DateTimeField()
    ended_at = models.DateTimeField(null=True, blank=True)

    ppg_green = models.JSONField(
        help_text="PPG green 배열",
    )

    x = models.JSONField(
        null=True,
        blank=True,
        help_text="IMU x 배열. 캘리브레이션 모드에서는 null",
    )

    y = models.JSONField(
        null=True,
        blank=True,
        help_text="IMU y 배열. 캘리브레이션 모드에서는 null",
    )

    z = models.JSONField(
        null=True,
        blank=True,
        help_text="IMU z 배열. 캘리브레이션 모드에서는 null",
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "sensor_window"
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["session"]),
            models.Index(fields=["started_at"]),
        ]

    def save(self, *args, **kwargs):
        if self.started_at and not self.ended_at:
            self.ended_at = self.started_at + timedelta(seconds=self.duration_sec)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"session {self.session_id} / {self.started_at}"