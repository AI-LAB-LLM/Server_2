import traceback
from datetime import timedelta

from analysis.models import Result

RISK_DETECTED_MIN_LEVEL = 4

OVERLAP_STEP_SEC = 6
WINDOW_GAP_TOLERANCE_SEC = 2


def build_samples_from_arrays(x, y, z):
    if x is None or y is None or z is None:
        return None

    if len(x) != len(y) or len(x) != len(z):
        return None

    return [[x[i], y[i], z[i]] for i in range(len(x))]


def build_samples_from_window(sensor_window):
    return build_samples_from_arrays(sensor_window.x, sensor_window.y, sensor_window.z)


def find_continuous_previous_window(sensor_window):
    from monitoring.models import SensorWindow

    previous = (
        SensorWindow.objects
        .filter(session_id=sensor_window.session_id, started_at__lt=sensor_window.started_at)
        .order_by("-started_at")
        .first()
    )

    if previous is None:
        return None

    gap = (sensor_window.started_at - previous.started_at).total_seconds()

    if abs(gap - SensorWindow.WINDOW_DURATION_SEC) > WINDOW_GAP_TOLERANCE_SEC:
        return None

    return previous


def save_imu_result(sensor_window, level, timestamp=None):
    return Result.objects.create(
        device_id=sensor_window.session.protectee.device_id,
        mode=sensor_window.session.mode,
        event_type=Result.EventType.IMU,
        timestamp=timestamp if timestamp is not None else sensor_window.started_at,
        probability=None,
        risk_level=level,
        risk_detected=level >= RISK_DETECTED_MIN_LEVEL,
    )


def _calculate_level(protectee_id, samples):
    try:
        from .calculator import EXPECTED_SAMPLE_COUNT, calculate_imu_level
    except ImportError as e:
        return None, {
            "imu_status": "error",
            "reason": f"calculator_dependency_missing: {e}",
        }

    if len(samples) != EXPECTED_SAMPLE_COUNT:
        return None, {
            "imu_status": "skipped",
            "reason": "sample_count_mismatch",
            "sample_count": len(samples),
            "expected_sample_count": EXPECTED_SAMPLE_COUNT,
        }

    try:
        result = calculate_imu_level(protectee_id, samples)
        return result, None
    except Exception as e:
        print("========== IMU CALCULATOR ERROR ==========")
        print(traceback.format_exc())
        print("========== IMU CALCULATOR ERROR END ==========")

        return None, {
            "imu_status": "error",
            "reason": str(e),
        }


def run_imu_level_for_window(sensor_window):
    # k=짝수: 윈도우 전체(12초, Wa/Wb/Wc...)를 그대로 계산 후 analysis_result에 저장

    samples = build_samples_from_window(sensor_window)

    if samples is None:
        return {
            "imu_status": "skipped",
            "reason": "imu_xyz_missing",
        }

    protectee_id = sensor_window.session.protectee_id
    result, error = _calculate_level(protectee_id, samples)

    if error is not None:
        return error

    result_obj = save_imu_result(sensor_window, result["level"], timestamp=sensor_window.started_at)

    return {
        "imu_status": "saved",
        "result_id": result_obj.id,
        "level": result_obj.risk_level,
        "risk_detected": result_obj.risk_detected,
        "probs": result["probs"],
    }


def run_imu_overlap_for_window(sensor_window):
    # k=홀수: 직전 윈도우 뒤 6초 + 현재 윈도우 앞 6초를 이어붙여 계산.
    # 직전 윈도우가 없거나(세션 첫 윈도우) 간격이 12초 근처가 아니면(유실/재시작) 건너뛴다.

    previous_window = find_continuous_previous_window(sensor_window)

    if previous_window is None:
        return {
            "imu_status": "skipped",
            "reason": "no_continuous_previous_window",
        }

    previous_samples = build_samples_from_window(previous_window)
    current_samples = build_samples_from_window(sensor_window)

    if previous_samples is None or current_samples is None:
        return {
            "imu_status": "skipped",
            "reason": "imu_xyz_missing",
        }

    try:
        from .calculator import EXPECTED_SAMPLE_COUNT
    except ImportError as e:
        return {
            "imu_status": "error",
            "reason": f"calculator_dependency_missing: {e}",
        }

    half = EXPECTED_SAMPLE_COUNT // 2
    samples = previous_samples[half:] + current_samples[:half]

    protectee_id = sensor_window.session.protectee_id
    result, error = _calculate_level(protectee_id, samples)

    if error is not None:
        return error

    overlap_timestamp = previous_window.started_at + timedelta(seconds=OVERLAP_STEP_SEC)
    result_obj = save_imu_result(sensor_window, result["level"], timestamp=overlap_timestamp)

    return {
        "imu_status": "saved",
        "result_id": result_obj.id,
        "level": result_obj.risk_level,
        "risk_detected": result_obj.risk_detected,
        "probs": result["probs"],
        "timestamp": overlap_timestamp.isoformat(),
    }
