import requests
from django.conf import settings

# 신변보호플랫폼(IMP) 측 event_type 코드. 명세서 3.7.4 기준
EVENT_TYPE_CODES = {
    "PPG": 1,
    "IMU": 2,
    "GEO": 3,
}

DANGER_EVENT_PATH = "/api/dts/event/danger"


class DangerPlatformError(Exception):
    pass


def send_danger_event(
    *,
    device_id: str,
    event_type: int,
    timestamp: int,
    threat_detected: bool,
    timeout: float = 5.0,
) -> dict:
    """
    위험정보 API(POST .../api/dts/event/danger) 호출.

    성공: {"SUCCESS": true}
    실패: {"SUCCESS": false, "MESSAGE": "..."}
    """
    url = f"{settings.DANGER_PLATFORM_BASE_URL}{DANGER_EVENT_PATH}"
    payload = {
        "device_id": device_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "threat_detected": threat_detected,
    }

    try:
        response = requests.post(url, json=payload, timeout=timeout)
    except requests.RequestException as e:
        raise DangerPlatformError(f"위험정보 API 요청 실패: {e}") from e

    try:
        data = response.json()
    except ValueError:
        raise DangerPlatformError(
            f"위험정보 API 응답 파싱 실패 (status={response.status_code})"
        )

    if not data.get("SUCCESS"):
        raise DangerPlatformError(data.get("MESSAGE", "위험정보 API 처리 실패"))

    return data
