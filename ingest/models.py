from django.db import models

class ThreatWindow(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)

    device_id = models.CharField(max_length=64)
    sos_id = models.CharField(max_length=128, blank=True, null=True)

    window_sec = models.PositiveSmallIntegerField()
    hz = models.PositiveSmallIntegerField()

    t_start = models.CharField(max_length=32)
    t_end = models.CharField(max_length=32)
    sample_count = models.PositiveSmallIntegerField()

    class Meta:
        indexes = [
            models.Index(fields=["device_id", "created_at"]),
            models.Index(fields=["sos_id"]),
        ]

    def __str__(self):
            return f"ThreatWindow(id={self.id}, device={self.device_id}, created_at={self.created_at})"

class ThreatSample(models.Model):
    window = models.ForeignKey(
        ThreatWindow, on_delete=models.CASCADE, related_name="samples"
    )

    # 서버가 수신 순서대로 0~149 자동 부여 
    seq = models.PositiveSmallIntegerField()

    # "00:00:00" 형태 time 형식 정해지면 TimeField로 변경 가능 
    time = models.CharField(max_length=32)

    ax = models.FloatField()
    ay = models.FloatField()
    az = models.FloatField()

    ppg_green = models.IntegerField()
    ppg_ir = models.IntegerField(null=True, blank=True)  # optional(ir,red)
    ppg_red = models.IntegerField(null=True, blank=True)  # optional

    class Meta:
        indexes = [
            models.Index(fields=["window", "seq"]),
        ]
        unique_together = ("window", "seq")  # window 내 순서 중복 방지

    def __str__(self):
        return f"ThreatSample(window={self.window_id}, seq={self.seq}, time={self.time})"