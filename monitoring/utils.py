from .models import Protectee, MonitoringSession


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


def get_active_session(protectee: Protectee, mode: str):
    """
    진행 중인 세션 조회.

    기준:
    - ended_at이 null이면 진행 중
    - ended_at에 값이 있으면 종료된 세션
    """
    return (
        MonitoringSession.objects
        .filter(
            protectee=protectee,
            mode=mode,
            ended_at__isnull=True,
        )
        .order_by("-started_at")
        .first()
    )


def get_or_create_session_for_sensor_data(
    protectee: Protectee,
    mode: str,
):
    """
    sensor-window 저장 시 사용할 세션을 조회하거나 생성.

    전제:
    - 중앙서버가 mode를 정확히 구분해서 전송함
    - THREAT, PERIODIC, CALIBRATION이 서로 겹쳐서 들어오지 않음

    처리:
    - 같은 mode의 진행 중 세션이 있으면 기존 세션 사용
    - 없으면 새 세션 생성
    """
    valid_modes = [
        MonitoringSession.Mode.THREAT,
        MonitoringSession.Mode.PERIODIC,
        MonitoringSession.Mode.CALIBRATION,
    ]

    if mode not in valid_modes:
        return None, {
            "detail": "mode는 THREAT, PERIODIC 또는 CALIBRATION이어야 합니다."
        }

    active_session = get_active_session(
        protectee=protectee,
        mode=mode,
    )

    if active_session:
        return active_session, None

    session = MonitoringSession.objects.create(
        protectee=protectee,
        mode=mode,
    )

    return session, None