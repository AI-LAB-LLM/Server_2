from django.utils import timezone
from .models import Protectee, MonitoringSession, SensorWindow


SESSION_TIMEOUT_SEC = 30


def normalize_device_id(device_id: str) -> str:
    if not device_id:
        return ""

    return device_id.strip()


def get_or_create_protectee_by_device_id(device_id: str) -> Protectee:
    normalized_device_id = normalize_device_id(device_id)

    if not normalized_device_id:
        raise ValueError("device_id는 필수입니다.")

    protectee, _ = Protectee.objects.get_or_create(
        device_id=normalized_device_id
    )

    return protectee


def _is_session_stale(session, new_started_at) -> bool:
    last_window = (
        SensorWindow.objects
        .filter(session=session)
        .order_by("-started_at")
        .values("started_at")
        .first()
    )

    if last_window is None:
        return False

    gap = (new_started_at - last_window["started_at"]).total_seconds()
    return gap > SESSION_TIMEOUT_SEC


def get_or_create_session_for_sensor_data(
    protectee: Protectee,
    mode: str,
    new_started_at=None,
):

    valid_modes = [
        MonitoringSession.Mode.THREAT,
        MonitoringSession.Mode.PERIODIC,
        MonitoringSession.Mode.CALIBRATION,
    ]

    if mode not in valid_modes:
        return None, {
            "detail": "mode는 THREAT, PERIODIC 또는 CALIBRATION이어야 합니다."
        }

    active_session = (
        MonitoringSession.objects
        .filter(
            protectee=protectee,
            mode=mode,
            ended_at__isnull=True,
        )
        .order_by("-started_at")
        .first()
    )

    if active_session:
        if new_started_at is None or not _is_session_stale(active_session, new_started_at):
            return active_session, None

        # 타임아웃된 세션은 닫고 새 세션 생성
        active_session.ended_at = timezone.now()
        active_session.save(update_fields=["ended_at"])

    session = MonitoringSession.objects.create(
        protectee=protectee,
        mode=mode,
    )

    return session, None