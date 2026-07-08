from datetime import datetime, timedelta, timezone as dt_timezone
from rest_framework import serializers
from .models import MonitoringSession

KST = dt_timezone(timedelta(hours=9))


class MonitoringSessionSerializer(serializers.ModelSerializer):
    protectee_id = serializers.IntegerField(source="protectee.id", read_only=True)
    device_id = serializers.CharField(source="protectee.device_id", read_only=True)
    protectee_name = serializers.CharField(source="protectee.name", read_only=True)
    is_active = serializers.SerializerMethodField()

    class Meta:
        model = MonitoringSession
        fields = [
            "id",
            "protectee_id",
            "device_id",
            "protectee_name",
            "mode",
            "is_active",
            "started_at",
            "ended_at",
        ]

    def get_is_active(self, obj):
        return obj.ended_at is None


MODE_CODE_TO_VALUE = {
    1: MonitoringSession.Mode.THREAT,
    2: MonitoringSession.Mode.PERIODIC,
    3: MonitoringSession.Mode.CALIBRATION,
}

MODE_VALUE_TO_CODE = {value: code for code, value in MODE_CODE_TO_VALUE.items()}


class SensorWindowCreateSerializer(serializers.Serializer):
    device_id = serializers.CharField(
        max_length=100,
        help_text="string, 전용 워치 device_id",
    )

    mode = serializers.ChoiceField(
        choices=list(MODE_CODE_TO_VALUE.keys()),
        help_text="number, 1=이벤트보고, 2=주기보고, 3=Calibration",
    )

    timestamp = serializers.IntegerField(
        help_text="integer, UNIX timestamp milliseconds. 예: 1777824000000",
    )

    sample_rate_hz = serializers.IntegerField(
        default=25,
        help_text="integer, 샘플링 주파수. 현재 25 고정",
    )

    duration_sec = serializers.IntegerField(
        default=12,
        help_text="integer, 데이터 길이 초 단위. 현재 12 고정",
    )

    imu = serializers.DictField(
        required=False,
        allow_empty=True,
        help_text="object, IMU 데이터. mode=1/2에서는 x, y, z 배열 필요. mode=3에서는 생략 가능. 각 배열은 최대 300개(일부 누락 가능)",
    )

    ppg_green = serializers.ListField(
        child=serializers.FloatField(),
        help_text="number[], PPG Green 센서 배열. 최대 300개(일부 누락 가능)",
    )

    def validate(self, attrs):
        mode = MODE_CODE_TO_VALUE[attrs["mode"]]
        attrs["mode"] = mode
        sample_rate_hz = attrs.get("sample_rate_hz", 25)
        duration_sec = attrs.get("duration_sec", 12)

        if sample_rate_hz != 25:
            raise serializers.ValidationError("sample_rate_hz는 25여야 합니다.")

        if duration_sec != 12:
            raise serializers.ValidationError("duration_sec는 12여야 합니다.")

        timestamp = attrs.get("timestamp")

        if timestamp is None:
            raise serializers.ValidationError("timestamp는 필수입니다.")

        attrs["started_at"] = datetime.fromtimestamp(
            timestamp / 1000,
            tz=KST,
        ).replace(tzinfo=None)

        expected_count = sample_rate_hz * duration_sec

        imu = attrs.get("imu") or {}
        ppg_green = attrs.get("ppg_green")

        if ppg_green is None:
            raise serializers.ValidationError("ppg_green 배열이 필요합니다.")

        if not isinstance(ppg_green, list):
            raise serializers.ValidationError("ppg_green은 배열이어야 합니다.")

        if len(ppg_green) > expected_count:
            raise serializers.ValidationError(
                f"ppg_green 배열 길이는 최대 {expected_count}개까지 허용됩니다."
            )

        if mode == MonitoringSession.Mode.CALIBRATION:
            attrs["x"] = None
            attrs["y"] = None
            attrs["z"] = None
            attrs["ppg_green"] = ppg_green
            return attrs

        x = imu.get("x")
        y = imu.get("y")
        z = imu.get("z")

        if x is None or y is None or z is None:
            raise serializers.ValidationError(
                "THREAT/PERIODIC 모드에서는 imu에 x, y, z 배열이 모두 필요합니다."
            )

        if not isinstance(x, list) or not isinstance(y, list) or not isinstance(z, list):
            raise serializers.ValidationError("imu.x, imu.y, imu.z는 배열이어야 합니다.")

        for name, arr in (("imu.x", x), ("imu.y", y), ("imu.z", z)):
            if len(arr) > expected_count:
                raise serializers.ValidationError(
                    f"{name} 배열 길이는 최대 {expected_count}개까지 허용됩니다."
                )

        attrs["x"] = x
        attrs["y"] = y
        attrs["z"] = z
        attrs["ppg_green"] = ppg_green

        return attrs