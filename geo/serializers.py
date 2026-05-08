from datetime import datetime, timezone as dt_timezone

from rest_framework import serializers

from monitoring.models import Protectee
from .models import GeoData


class GeoDataCreateSerializer(serializers.Serializer):
    device_id = serializers.CharField(
        max_length=100,
        help_text="string, 전용 워치 device_id",
    )

    timestamp = serializers.IntegerField(
        help_text="integer, GPS 측정 시간, UNIX timestamp milliseconds",
    )

    latitude = serializers.FloatField(
        help_text="double, 위도",
    )

    longitude = serializers.FloatField(
        help_text="double, 경도",
    )

    def validate_device_id(self, value):
        value = value.strip()

        if not value:
            raise serializers.ValidationError("device_id는 비어 있을 수 없습니다.")

        return value

    def validate_timestamp(self, value):
        if value <= 0:
            raise serializers.ValidationError("timestamp는 양수 UNIX ms 값이어야 합니다.")

        # UNIX seconds가 실수로 들어오는 것 방지
        # UNIX seconds 예: 1714896000
        # UNIX milliseconds 예: 1714896000000
        if value < 10**12:
            raise serializers.ValidationError(
                "timestamp는 UNIX seconds가 아니라 UNIX milliseconds여야 합니다."
            )

        return value

    def validate_latitude(self, value):
        if value < -90 or value > 90:
            raise serializers.ValidationError("latitude는 -90 이상 90 이하이어야 합니다.")

        return value

    def validate_longitude(self, value):
        if value < -180 or value > 180:
            raise serializers.ValidationError("longitude는 -180 이상 180 이하이어야 합니다.")

        return value

    def create(self, validated_data):
        device_id = validated_data["device_id"]
        timestamp_ms = validated_data["timestamp"]

        timestamp_utc = datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=dt_timezone.utc,
        )

        protectee, _ = Protectee.objects.get_or_create(
            device_id=device_id,
        )

        geo_data = GeoData.objects.create(
            protectee=protectee,
            device_id=device_id,
            timestamp=timestamp_utc,
            latitude=validated_data["latitude"],
            longitude=validated_data["longitude"],
        )

        return geo_data


class GeoDataResponseSerializer(serializers.ModelSerializer):
    protectee_id = serializers.IntegerField(
        source="protectee.id",
        read_only=True,
    )

    class Meta:
        model = GeoData
        fields = [
            "id",
            "protectee_id",
            "device_id",
            "timestamp",
            "latitude",
            "longitude",
            "created_at",
        ]