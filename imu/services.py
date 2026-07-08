import traceback

from analysis.models import Result

RISK_DETECTED_MIN_LEVEL = 4


def build_samples_from_window(sensor_window):

    x = sensor_window.x
    y = sensor_window.y
    z = sensor_window.z

    if x is None or y is None or z is None:
        return None

    if len(x) != len(y) or len(x) != len(z):
        return None

    return [[x[i], y[i], z[i]] for i in range(len(x))]


def save_imu_result(sensor_window, level):
    return Result.objects.create(
        device_id=sensor_window.session.protectee.device_id,
        mode=sensor_window.session.mode,
        event_type=Result.EventType.IMU,
        timestamp=sensor_window.started_at,
        probability=None,
        risk_level=level,
        risk_detected=level >= RISK_DETECTED_MIN_LEVEL,
    )


def run_imu_level_for_window(sensor_window):
    # imu level  계산 후 analysis_result에 저장

    samples = build_samples_from_window(sensor_window)

    if samples is None:
        return {
            "imu_status": "skipped",
            "reason": "imu_xyz_missing",
        }

    try:
        from .calculator import EXPECTED_SAMPLE_COUNT, calculate_imu_level
    except ImportError as e:
        return {
            "imu_status": "error",
            "reason": f"calculator_dependency_missing: {e}",
        }

    if len(samples) != EXPECTED_SAMPLE_COUNT:
        return {
            "imu_status": "skipped",
            "reason": "sample_count_mismatch",
            "sample_count": len(samples),
            "expected_sample_count": EXPECTED_SAMPLE_COUNT,
        }

    try:
        protectee_id = sensor_window.session.protectee_id
        result = calculate_imu_level(protectee_id, samples)
        result_obj = save_imu_result(sensor_window, result["level"])

        return {
            "imu_status": "saved",
            "result_id": result_obj.id,
            "level": result_obj.risk_level,
            "risk_detected": result_obj.risk_detected,
            "probs": result["probs"],
        }

    except Exception as e:
        print("========== IMU CALCULATOR ERROR ==========")
        print(traceback.format_exc())
        print("========== IMU CALCULATOR ERROR END ==========")

        return {
            "imu_status": "error",
            "reason": str(e),
        }
