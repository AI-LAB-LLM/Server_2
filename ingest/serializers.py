from rest_framework import serializers

class IngestResponseSerializer(serializers.Serializer):
    ok = serializers.BooleanField()
    window_id = serializers.IntegerField()
    saved_samples = serializers.IntegerField()


class SampleSerializer(serializers.Serializer):
    time = serializers.CharField(max_length=32)
    ax = serializers.FloatField()
    ay = serializers.FloatField()
    az = serializers.FloatField()
    ppg_green = serializers.IntegerField()
    ppg_ir = serializers.IntegerField(required=False, allow_null=True)
    ppg_red = serializers.IntegerField(required=False, allow_null=True)

    def validate(self, attrs):
        attrs["ppg_ir"] = attrs.get("ppg_ir", None)
        attrs["ppg_red"] = attrs.get("ppg_red", None)
        return attrs


class IngestSerializer(serializers.Serializer):
    device_id = serializers.CharField(max_length=64)
    sos_id = serializers.CharField(
        max_length=128, required=False, allow_null=True, allow_blank=True
    )

    window_sec = serializers.IntegerField(required=False)
    hz = serializers.IntegerField(required=False)

    samples = SampleSerializer(many=True)

    def validate(self, attrs):
        samples = attrs.get("samples", [])

        if len(samples) < 1:
            raise serializers.ValidationError(
                {"samples": "At least 1 sample is required."}
            )

        for i, s in enumerate(samples):
            if not str(s.get("time", "")).strip():
                raise serializers.ValidationError(
                    {"samples": f"sample[{i}].time is empty."}
                )

        return attrs
