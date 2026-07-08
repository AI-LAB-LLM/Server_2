from datetime import datetime, timedelta, timezone
from rest_framework import serializers

KST = timezone(timedelta(hours=9))


class UnixMsDateTimeField(serializers.Field):

    default_error_messages = {
        "invalid": "timestamp는 UNIX time(ms) 형식의 정수여야 합니다."
    }

    def to_internal_value(self, value):
        try:
            ts_ms = int(value)
            ts_sec = ts_ms / 1000.0
            return datetime.fromtimestamp(ts_sec, tz=KST).replace(tzinfo=None)
        except (TypeError, ValueError, OSError, OverflowError):
            self.fail("invalid")

    def to_representation(self, value):
        return int(value.replace(tzinfo=KST).timestamp() * 1000)