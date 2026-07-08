from django.db import models


class ApneaSession(models.Model):
    """baseline 저장 전용. monitoring.SensorWindow와 별도로 관리."""
    device_id      = models.CharField(max_length=100, db_index=True)
    started_at     = models.DateTimeField()
    baseline_ready = models.BooleanField(default=False)
    baseline_stats = models.JSONField(null=True, blank=True)
    created_at     = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self):
        return f"ApneaSession({self.device_id} @ {self.started_at.isoformat()})"


class ApneaResult(models.Model):
    """monitoring.SensorWindow 처리 결과 저장."""
    sensor_window = models.OneToOneField(
        'monitoring.SensorWindow',
        on_delete=models.CASCADE,
        related_name='apnea_result',
    )
    device_id      = models.CharField(max_length=100, db_index=True)
    processed_at   = models.DateTimeField(auto_now_add=True)

    wear_valid     = models.BooleanField(null=True)
    wear_label     = models.IntegerField(null=True)
    is_baseline    = models.BooleanField(default=False)
    beat_results   = models.JSONField(null=True, blank=True)
    p_apnea        = models.FloatField(null=True)
    p_apnea_smooth = models.FloatField(null=True)
    pred_label     = models.IntegerField(null=True)
    pred_status    = models.CharField(max_length=32, null=True, blank=True)

    class Meta:
        db_table = 'apnea_result'
        ordering = ['-processed_at']

    def __str__(self):
        return f"ApneaResult(window={self.sensor_window_id}, device={self.device_id})"