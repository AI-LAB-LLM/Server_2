import argparse
import time
from django.core.management.base import BaseCommand, CommandError
from analysis.platform_client import (
    EVENT_TYPE_CODES,
    DangerPlatformError,
    send_danger_event,
)

# 위협 감지 알고리즘 완성 전, 플랫폼과의 데이터 송수신 연동을 확인하기 위한 용도
class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--device-id",
            required=True,
            help="전용 워치 device_id",
        )
        parser.add_argument(
            "--event-type",
            required=True,
            choices=sorted(EVENT_TYPE_CODES),
            help="PPG, IMU, GEO 중 하나",
        )
        parser.add_argument(
            "--timestamp",
            type=int,
            default=None,
            help="UNIX timestamp(ms). 미지정 시 현재 시각 사용",
        )
        parser.add_argument(
            "--threat-detected",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="threat_detected 값 (기본값: True)",
        )

    def handle(self, *args, **options):
        device_id = options["device_id"]
        event_type = EVENT_TYPE_CODES[options["event_type"]]
        timestamp = options["timestamp"] or int(time.time() * 1000)
        threat_detected = options["threat_detected"]

        self.stdout.write(
            f"전송 요청: device_id={device_id}, event_type={event_type}"
            f"({options['event_type']}), timestamp={timestamp}, "
            f"threat_detected={threat_detected}"
        )

        try:
            result = send_danger_event(
                device_id=device_id,
                event_type=event_type,
                timestamp=timestamp,
                threat_detected=threat_detected,
            )
        except DangerPlatformError as e:
            raise CommandError(str(e))

        self.stdout.write(self.style.SUCCESS(f"전송 성공: {result}"))
