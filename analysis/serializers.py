from rest_framework import serializers

from .models import Result


class ResultCreateSerializer(serializers.Serializer):
    device_id = serializers.CharField(
        max_length=100,
        help_text="string, 전용 워치 device_id",
    )

    mode = serializers.ChoiceField(
        choices=Result.Mode.choices,
        help_text="string, THREAT=상시보고, PERIODIC=주기보고",
    )

    event_type = serializers.ChoiceField(
        choices=Result.EventType.choices,
        help_text="string, PPG 또는 IMU",
    )

    timestamp = serializers.IntegerField(
        help_text="integer, UNIX timestamp milliseconds. 분석 결과 기준 시각",
    )

    probability = serializers.FloatField(
        required=False,
        allow_null=True,
        min_value=0.0,
        max_value=1.0,
        help_text="number 또는 null, 위험 확률값",
    )

    risk_level = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=1,
        max_value=5,
        help_text="integer 또는 null, 1~5 위험도 등급",
    )

    risk_detected = serializers.BooleanField(
        required=False,
        allow_null=True,
        help_text="boolean 또는 null, 위험 감지 여부",
    )


class ResultSerializer(serializers.ModelSerializer):
    class Meta:
        model = Result
        fields = [
            "id",
            "device_id",
            "mode",
            "event_type",
            "timestamp",
            "probability",
            "risk_level",
            "risk_detected",
            "created_at",
        ]