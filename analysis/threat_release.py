import logging
from datetime import timedelta, timezone as dt_timezone

from django.utils import timezone

from .models import Result
from .platform_client import EVENT_TYPE_CODES, send_danger_event

logger = logging.getLogger(__name__)

KST = dt_timezone(timedelta(hours=9))

LATCHED_EVENT_TYPES = (Result.EventType.PPG, Result.EventType.IMU)

STALE_CLEAR_LOG = 3


def clear_latched_threats(device_id):
    cleared = []

    for event_type in LATCHED_EVENT_TYPES:
        last_detected = (
            Result.objects
            .filter(device_id=device_id, event_type=event_type)
            .order_by("-created_at")
            .values_list("threat_detected", flat=True)
            .first()
        )

        if not last_detected:
            continue

        sent_at = timezone.now()

        try:
            send_danger_event(
                device_id=device_id,
                event_type=EVENT_TYPE_CODES[event_type],
                timestamp=int(sent_at.replace(tzinfo=KST).timestamp() * 1000),
                threat_detected=False,
                threat_detected_log=STALE_CLEAR_LOG,
            )
        except Exception as e:
            logger.error(
                f"[clear] {event_type} 해제 전송 실패 (device={device_id}): {e}"
            )
            continue

        cleared.append(event_type)
        logger.info(
            f"[clear] {event_type} 해제 전송 device={device_id} "
            f"(threat_detected=False, log={STALE_CLEAR_LOG})"
        )

    return cleared


def release_stale_device_threats(cutoff):
    from monitoring.models import MonitoringSession

    pending = MonitoringSession.objects.filter(
        threat_cleared_at__isnull=True,
        last_received_at__isnull=False,
        last_received_at__lt=cutoff,
        mode__in=[
            MonitoringSession.Mode.THREAT,
            MonitoringSession.Mode.PERIODIC,
        ],
    )

    device_ids = sorted(set(pending.values_list("protectee__device_id", flat=True)))
    released = 0

    for device_id in device_ids:
        still_active = (
            MonitoringSession.objects
            .filter(
                protectee__device_id=device_id,
                last_received_at__gte=cutoff,
            )
            .exists()
        )

        if still_active:
            continue

        pks = list(
            pending
            .filter(protectee__device_id=device_id)
            .values_list("pk", flat=True)
        )

        claimed = (
            MonitoringSession.objects
            .filter(pk__in=pks, threat_cleared_at__isnull=True)
            .update(threat_cleared_at=timezone.now())
        )

        if not claimed:
            continue

        clear_latched_threats(device_id)
        released += 1

    return released
