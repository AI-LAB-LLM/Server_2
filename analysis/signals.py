import logging
from datetime import timedelta, timezone as dt_timezone
from django.db import transaction
from .models import Result
from .platform_client import EVENT_TYPE_CODES, send_danger_event
from .sos_services import SOS_SOURCE_EVENT_TYPES, handle_sos_evaluation

logger = logging.getLogger(__name__)

KST = dt_timezone(timedelta(hours=9))


def handle_result_created(sender, instance, created, **kwargs):
    """analysis_result에 새 결과가 쌓이면 그 즉시 플랫폼으로 전송."""
    if not created:
        return

    if instance.event_type == Result.EventType.SOS:
        return

    _schedule_platform_send(instance)

    if instance.event_type in SOS_SOURCE_EVENT_TYPES:
        device_id = instance.device_id
        mode = instance.mode
        timestamp = instance.timestamp

        transaction.on_commit(
            lambda: handle_sos_evaluation(
                device_id=device_id,
                mode=mode,
                timestamp=timestamp,
            )
        )


def _schedule_platform_send(instance):
    if instance.threat_detected is None:
        return

    event_type = EVENT_TYPE_CODES.get(instance.event_type)
    if event_type is None:
        logger.warning(f"[danger] 전송 제외: 알 수 없는 event_type={instance.event_type}")
        return

    timestamp = int(instance.created_at.replace(tzinfo=KST).timestamp() * 1000)

    device_id = instance.device_id
    threat_detected = instance.threat_detected
    threat_detected_log = instance.threat_detected_log
    result_id = instance.id

    transaction.on_commit(
        lambda: _send_danger_event(
            result_id=result_id,
            device_id=device_id,
            event_type=event_type,
            timestamp=timestamp,
            threat_detected=threat_detected,
            threat_detected_log=threat_detected_log,
        )
    )


def _send_danger_event(
    *,
    result_id,
    device_id,
    event_type,
    timestamp,
    threat_detected,
    threat_detected_log,
):
    try:
        send_danger_event(
            device_id=device_id,
            event_type=event_type,
            timestamp=timestamp,
            threat_detected=threat_detected,
            threat_detected_log=threat_detected_log,
        )
    except Exception as e:
        logger.error(f"[danger] 전송 실패 (result_id={result_id}): {e}")
        return

    logger.info(
        f"[danger] 전송 성공 (result_id={result_id}) "
        f"device={device_id} event_type={event_type} "
        f"threat_detected={threat_detected} log={threat_detected_log}"
    )
