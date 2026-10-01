"""PPG/IMU 개별 위협감지 결과를 최근 30초 누적해 SOS 여부를 판정한다.

- 판정 시점: 새 PPG 또는 IMU 결과가 저장될 때마다
- 누적 창: (t - 30s, t]. t는 방금 저장된 결과의 timestamp(= 구간 시작 시각)
- 전송: 한 모니터링 세션당 최초 1회. 같은 세션 안에서는 SOS가 풀렸다 다시 성립해도
  재전송하지 않는다. 해제(true -> false)도 전송하지 않는다.

PPG/IMU 위협 상태 해제는 데이터 끊김 시점에 처리하므로 threat_release.py에 있다.
"""

import logging
from datetime import timedelta, timezone as dt_timezone

from django.utils import timezone

from .models import Result
from .platform_client import EVENT_TYPE_CODES, send_danger_event

logger = logging.getLogger(__name__)

KST = dt_timezone(timedelta(hours=9))

# SOS 판정에 사용하는 입력 이벤트
SOS_SOURCE_EVENT_TYPES = (Result.EventType.PPG, Result.EventType.IMU)

SOS_WINDOW_SEC = 30

# ① 물리적 이상 + 심리적 이상
PPG_TRUE_MIN_WITH_IMU = 1
IMU_TRUE_CONSECUTIVE_MIN = 3

# ② 정적인 상태 + 지속적인 심리적 이상
PPG_TRUE_MIN_ALONE = 3


def _window_flags(device_id, event_type, window_start, t):
    """(window_start, t] 구간 결과의 threat_detected를 시간 순으로."""
    return list(
        Result.objects
        .filter(
            device_id=device_id,
            event_type=event_type,
            timestamp__gt=window_start,
            timestamp__lte=t,
        )
        .order_by("timestamp")
        .values_list("threat_detected", flat=True)
    )


def _max_consecutive_true(flags):
    """analysis_result에 저장된 row 순서 기준 최대 연속 true 길이."""
    best = 0
    run = 0

    for flag in flags:
        if flag:
            run += 1
            best = max(best, run)
        else:
            run = 0

    return best


def evaluate_sos(device_id, t):
    """최근 30초 누적 결과로 SOS 여부를 판정. (detected, detail) 반환."""
    window_start = t - timedelta(seconds=SOS_WINDOW_SEC)

    ppg_flags = _window_flags(device_id, Result.EventType.PPG, window_start, t)
    imu_flags = _window_flags(device_id, Result.EventType.IMU, window_start, t)

    ppg_true = sum(1 for flag in ppg_flags if flag)
    imu_streak = _max_consecutive_true(imu_flags)

    detail = {
        "ppg_true": ppg_true,
        "ppg_count": len(ppg_flags),
        "imu_streak": imu_streak,
        "imu_count": len(imu_flags),
    }

    if ppg_true >= PPG_TRUE_MIN_WITH_IMU and imu_streak >= IMU_TRUE_CONSECUTIVE_MIN:
        detail["rule"] = "physical+psychological"
        return True, detail

    if ppg_true >= PPG_TRUE_MIN_ALONE:
        detail["rule"] = "psychological"
        return True, detail

    detail["rule"] = None
    return False, detail


def _resolve_session(device_id, mode):
    """판정을 유발한 결과가 속한 모니터링 세션."""
    from monitoring.models import MonitoringSession

    sessions = MonitoringSession.objects.filter(
        protectee__device_id=device_id,
        mode=mode,
    )

    active = sessions.filter(ended_at__isnull=True).order_by("-started_at").first()
    if active is not None:
        return active

    # PPG 청크는 현재 윈도우보다 6~12초 뒤처져 저장되므로, 세션이 막 닫힌 뒤에
    # 판정이 돌 수 있다. 그때는 방금 닫힌 세션을 그대로 쓴다.
    return sessions.order_by("-started_at").first()


def _claim_session_sos(session):
    """세션의 SOS 전송 권한을 선점. 이미 전송한 세션이면 False."""
    from monitoring.models import MonitoringSession

    claimed = (
        MonitoringSession.objects
        .filter(pk=session.pk, sos_detected_at__isnull=True)
        .update(sos_detected_at=timezone.now())
    )

    return claimed == 1


def _release_session_sos(session):
    """전송에 실패했으면 선점을 되돌려 다음 판정에서 다시 시도하게 한다."""
    from monitoring.models import MonitoringSession

    MonitoringSession.objects.filter(pk=session.pk).update(sos_detected_at=None)


def handle_sos_evaluation(*, device_id, mode, timestamp):
    """새 PPG/IMU 결과가 커밋된 뒤 호출. 세션당 최초 1회만 기록하고 전송한다."""
    try:
        detected, detail = evaluate_sos(device_id, timestamp)
    except Exception as e:
        logger.error(f"[sos] 판정 실패 (device={device_id}): {e}")
        return

    if not detected:
        return

    session = _resolve_session(device_id, mode)

    if session is None:
        logger.warning(
            f"[sos] 세션을 찾지 못해 전송 생략 device={device_id} mode={mode}"
        )
        return

    if not _claim_session_sos(session):
        logger.debug(
            f"[sos] 이미 전송한 세션 - 생략 (session_id={session.id}) "
            f"device={device_id} {detail}"
        )
        return

    sent_at = timezone.now()
    event_timestamp = int(sent_at.replace(tzinfo=KST).timestamp() * 1000)

    try:
        send_danger_event(
            device_id=device_id,
            event_type=EVENT_TYPE_CODES["SOS"],
            timestamp=event_timestamp,
            threat_detected=True,
            threat_detected_log=None,
        )
    except Exception as e:
        _release_session_sos(session)
        logger.error(f"[sos] 전송 실패 (session_id={session.id}): {e}")
        return

    sos_result = Result.objects.create(
        device_id=device_id,
        mode=mode,
        event_type=Result.EventType.SOS,
        timestamp=timestamp,
        probability=None,
        threat_detected=True,
        threat_detected_log=None,
    )

    logger.info(
        f"[sos] 전송 성공 (result_id={sos_result.id}, session_id={session.id}) "
        f"device={device_id} rule={detail['rule']} "
        f"ppg_true={detail['ppg_true']}/{detail['ppg_count']} "
        f"imu_streak={detail['imu_streak']}/{detail['imu_count']}"
    )
