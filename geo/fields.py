from datetime import datetime, timezone
from rest_framework import serializers


class UnixMsDateTimeField(serializers.Field):

    default_error_messages = {
        "invalid": "timestamp는 UNIX time(ms) 형식의 정수여야 합니다."
    }

    def to_internal_value(self, value):
        try:
            ts_ms = int(value)
            ts_sec = ts_ms / 1000.0
            return datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            self.fail("invalid")

    def to_representation(self, value):
        return int(value.timestamp() * 1000)