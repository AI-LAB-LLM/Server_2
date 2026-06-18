from pathlib import Path
from datetime import timedelta
import pandas as pd
import numpy as np
import traceback
from django.conf import settings
from .models import GeoProcessedData
from .gpr_runtime import GPRRuntime, haversine_m, MOVE_SPEED_THRESHOLD_MPS
from .anomaly_services import run_anomaly_for_latest


# GPS 들어올 때마다 실행되는 보정 로직

# GEO 모델 설정
GEO_MODEL_DEVICE_ID = "212e15388f880450"
GPR_VERSION = "0612"

GEO_MODEL_DIR = (
    Path(settings.BASE_DIR)
    / "media"
    / "models"
    / "geo"
)

_GPR_RUNTIME_CACHE = {}


def get_gpr_runtime(device_id):
    """
    GPS가 들어올 때마다 bundle을 다시 joblib.load 하지 않도록
    GPRRuntime을 메모리에 캐싱한다.
    """
    device_id = str(device_id)
    cache_key = (device_id, GPR_VERSION)

    if cache_key in _GPR_RUNTIME_CACHE:
        return _GPR_RUNTIME_CACHE[cache_key]

    gpr = GPRRuntime(
        model_dir=str(GEO_MODEL_DIR),
        version=GPR_VERSION,
        device_id=device_id,
    )

    _GPR_RUNTIME_CACHE[cache_key] = gpr
    return gpr

# =========================
# 공통 유틸
# =========================

def safe_value(value):
    """
    pandas/numpy 값을 Django DB에 저장 가능한 Python 기본 타입으로 변환.
    - NaN, NaT, None -> None
    - numpy scalar -> Python scalar
    - empty ndarray/list/tuple -> None
    - size 1 ndarray/list/tuple -> 내부 값 1개로 변환
    - size 2 이상 ndarray/list/tuple -> 문자열로 변환
    """
    if value is None:
        return None

    # numpy array 처리
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return None
        if value.size == 1:
            return safe_value(value.item())
        return str(value.tolist())

    # list / tuple 처리
    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            return None
        if len(value) == 1:
            return safe_value(value[0])
        return str(value)

    # numpy scalar 처리
    if isinstance(value, np.generic):
        return safe_value(value.item())

    # pandas NaN / NaT 처리
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def safe_device_id(device_id):
    return str(device_id).replace("/", "_").replace("\\", "_").replace(":", "_")


def get_gpr_bundle_path(device_id):
    safe_id = safe_device_id(device_id)
    return GEO_MODEL_DIR / f"gpr_bundle_{GPR_VERSION}_device_{safe_id}.joblib"

#모델 파일 존재 확인
def check_gpr_model_files(device_id):
    bundle_path = get_gpr_bundle_path(device_id)

    if not bundle_path.exists():
        return [str(bundle_path)]

    return []


def build_recent_gps_dataframe(device_id, reference_time, minutes=60):
    """
    GeoProcessedData에서 특정 시점 기준 최근 60분 GPS 데이터를 조회해서
    GPRRuntime 입력 형식의 DataFrame으로 변환.

    gpr_runtime.py가 요구하는 주요 컬럼:
    - device_id
    - Timestamp
    - Latitude
    - longitude

    주의:
    - 여기서 Latitude / longitude는 DB의 최종 보정 좌표가 아니라
      raw_latitude / raw_longitude를 넣는다.
    - gpr_runtime.py 내부에서 Raw_Latitude / Raw_longitude를 보존하고,
      Latitude / longitude를 working 좌표로 사용한다.
    """

    start_time = reference_time - timedelta(minutes=minutes)

    qs = (
        GeoProcessedData.objects.filter(
            device_id=device_id,
            timestamp__gte=start_time,
            timestamp__lte=reference_time,
        )
        .order_by("timestamp")
        .values(
            "device_id",
            "timestamp",
            "raw_latitude",
            "raw_longitude",
        )
    )

    rows = []
    for row in qs:
        rows.append(
            {
                "device_id": row["device_id"],
                "Timestamp": row["timestamp"],
                "Latitude": row["raw_latitude"],
                "longitude": row["raw_longitude"],
            }
        )

    return pd.DataFrame(rows)


def save_raw_as_final_for_unsupported_device(geo_obj):
    """
    현재 모델이 없는 device_id일 경우 GPR을 실행하지 않고,
    raw GPS를 최종 지도 좌표로 저장한다.

    이유:
    - 현재 GPR 모델 파일은 GEO_MODEL_DEVICE_ID 전용이다.
    - 다른 device_id에 해당 모델을 적용하면 잘못된 보정이 될 수 있다.
    """

    geo_obj.latitude = geo_obj.raw_latitude
    geo_obj.longitude = geo_obj.raw_longitude

    geo_obj.gps_quality = "UNSUPPORTED_DEVICE"
    geo_obj.gps_filter_decision = "model_not_available"
    geo_obj.use_raw_for_gpr = False
    geo_obj.interp_method = ""

    geo_obj.predicted_latitude = None
    geo_obj.predicted_longitude = None
    geo_obj.predicted_uncertainty_m = None
    geo_obj.predicted_confidence_level = None

    geo_obj.state_primary = None

    geo_obj.save()

    return {
        "gpr_status": "skipped",
        "reason": "unsupported_device",
        "message": (
            "현재 해당 device_id에 대한 GPR 모델 파일이 없어 "
            "GPR 보정은 수행하지 않고 raw 좌표를 최종 좌표로 저장했습니다."
        ),
        "device_id": geo_obj.device_id,
        "model_device_id": GEO_MODEL_DEVICE_ID,
        "corrected_latitude": geo_obj.latitude,
        "corrected_longitude": geo_obj.longitude,
        "gps_quality": geo_obj.gps_quality,
        "gps_filter_decision": geo_obj.gps_filter_decision,
        "use_raw_for_gpr": geo_obj.use_raw_for_gpr,
        "interp_method": geo_obj.interp_method,
        "predicted_latitude": geo_obj.predicted_latitude,
        "predicted_longitude": geo_obj.predicted_longitude,
        "predicted_uncertainty_m": geo_obj.predicted_uncertainty_m,
        "predicted_confidence_level": geo_obj.predicted_confidence_level,
        "state_primary": geo_obj.state_primary,
    }


# =========================
# GPR 실행 및 DB 업데이트
# =========================

def run_gpr_and_update_latest(geo_obj):
    """
    방금 저장된 GeoProcessedData row를 기준으로 최근 60분 데이터를 조회하고,
    GPRRuntime을 실행한 뒤, 가장 마지막 행 결과를 geo_obj에 업데이트한다.

    geo_obj:
        방금 생성한 GeoProcessedData 객체

    반환:
        API 응답에 넣을 수 있는 dict
    """

    # 현재는 19395f6a434f4ca6 전용 모델만 있으므로,
    # 다른 device_id에는 GPR 모델을 적용하지 않는다.
    if geo_obj.device_id != GEO_MODEL_DEVICE_ID:
        return save_raw_as_final_for_unsupported_device(geo_obj)

    recent_df = build_recent_gps_dataframe(
        device_id=geo_obj.device_id,
        reference_time=geo_obj.timestamp,
        minutes=60,
    )

    if recent_df.empty:
        return {
            "gpr_status": "skipped",
            "reason": "recent_df_empty",
        }

    missing_files = check_gpr_model_files(geo_obj.device_id)
    if missing_files:
        return {
            "gpr_status": "skipped",
            "reason": "model_file_missing",
            "missing_files": missing_files,
        }

    try:
        gpr = get_gpr_runtime(geo_obj.device_id)

        processed_df = gpr.preprocess_and_predict(recent_df)

        if processed_df.empty:
            return {
                "gpr_status": "skipped",
                "reason": "processed_df_empty",
            }

        latest = processed_df.iloc[-1]

        # 최종 지도 표시용 좌표
        # GPRRuntime 처리 후 나온 최종 좌표를 저장
        geo_obj.latitude = safe_value(latest.get("Latitude"))
        geo_obj.longitude = safe_value(latest.get("longitude"))

        # GPRRuntime 처리 결과
        geo_obj.gps_quality = safe_value(latest.get("gps_quality"))
        geo_obj.gps_filter_decision = safe_value(latest.get("gps_filter_decision"))
        geo_obj.use_raw_for_gpr = safe_value(latest.get("use_raw_for_gpr"))
        geo_obj.interp_method = safe_value(latest.get("interp_method"))

        geo_obj.predicted_latitude = safe_value(latest.get("Predicted_Latitude"))
        geo_obj.predicted_longitude = safe_value(latest.get("Predicted_longitude"))
        geo_obj.predicted_uncertainty_m = safe_value(
            latest.get("Predicted_uncertainty_m")
        )
        geo_obj.predicted_confidence_level = safe_value(
            latest.get("Predicted_confidence_level")
        )

        geo_obj.state_primary = safe_value(latest.get("state_primary"))

        geo_obj.save()

        return {
            "gpr_status": "ok",
            "geo_processed_id": geo_obj.id,
            "corrected_latitude": geo_obj.latitude,
            "corrected_longitude": geo_obj.longitude,
            "gps_quality": geo_obj.gps_quality,
            "gps_filter_decision": geo_obj.gps_filter_decision,
            "use_raw_for_gpr": geo_obj.use_raw_for_gpr,
            "interp_method": geo_obj.interp_method,
            "predicted_latitude": geo_obj.predicted_latitude,
            "predicted_longitude": geo_obj.predicted_longitude,
            "predicted_uncertainty_m": geo_obj.predicted_uncertainty_m,
            "predicted_confidence_level": geo_obj.predicted_confidence_level,
            "state_primary": geo_obj.state_primary,
        }

    except Exception as e:
        print("========== GPR ERROR ==========")
        print(traceback.format_exc())
        print("========== GPR ERROR END ==========")

        return {
            "gpr_status": "error",
            "reason": str(e),
            "traceback": traceback.format_exc(),
        }


def create_geo_processed_data_and_run_gpr(
    protectee,
    device_id,
    timestamp,
    latitude,
    longitude,
):
    if str(device_id) != GEO_MODEL_DEVICE_ID:
        return None, {"gpr_status": "skipped", "reason": "unsupported_device"}, {"anomaly_status": "skipped", "reason": "unsupported_device"}

    pos_success = latitude is not None and longitude is not None

    geo_obj = GeoProcessedData.objects.create(
        protectee=protectee,
        device_id=device_id,
        timestamp=timestamp,

        raw_latitude=latitude,
        raw_longitude=longitude,

        latitude=None,
        longitude=None,

        pos_success=pos_success,
    )

    gpr_result = run_gpr_and_update_latest(geo_obj)

    geo_obj.refresh_from_db()

    anomaly_result = run_anomaly_for_latest(
        geo_obj=geo_obj,
        minutes=180,
    )

    return geo_obj, gpr_result, anomaly_result


# =========================
# Backfill 전용 후처리
# =========================

def fill_remaining_gaps_with_linear_interpolation(device_id):
    """
    Backfill 전용 후처리.

    실시간 처리(run_gpr_and_update_latest)는 reference_time 이전 데이터만 보기 때문에
    처리 대상 row가 항상 조회 윈도우의 마지막 row가 되어, 그 이후의 정상 좌표를
    이용한 선형보간이 불가능하다. 그 결과 latitude/longitude가 NULL인 채로
    gps_filter_decision=gpr_fill_needed로 남는 row가 생길 수 있다.

    backfill 시점에는 전체 geo_processed_data가 이미 채워져 있으므로,
    이런 row를 기준으로 가장 가까운 이전/이후 정상 좌표 사이를 시간 가중
    선형보간하여 latitude/longitude를 채운다.
    """

    rows = list(
        GeoProcessedData.objects.filter(device_id=device_id).order_by("timestamp")
    )

    filled = 0

    for i, row in enumerate(rows):
        if row.latitude is not None and row.longitude is not None:
            continue

        prev_row = next(
            (
                rows[j]
                for j in range(i - 1, -1, -1)
                if rows[j].latitude is not None and rows[j].longitude is not None
            ),
            None,
        )
        next_row = next(
            (
                rows[j]
                for j in range(i + 1, len(rows))
                if rows[j].latitude is not None and rows[j].longitude is not None
            ),
            None,
        )

        if prev_row is None or next_row is None:
            continue

        total_seconds = (next_row.timestamp - prev_row.timestamp).total_seconds()
        if total_seconds <= 0:
            continue

        alpha = (row.timestamp - prev_row.timestamp).total_seconds() / total_seconds
        alpha = min(max(alpha, 0.0), 1.0)

        row.latitude = prev_row.latitude + alpha * (next_row.latitude - prev_row.latitude)
        row.longitude = prev_row.longitude + alpha * (next_row.longitude - prev_row.longitude)

        row.gps_quality = "LOW"
        row.gps_filter_decision = "linear_filled_backfill"
        row.use_raw_for_gpr = False
        row.interp_method = "linear_fallback"

        dist_m = haversine_m(
            prev_row.latitude, prev_row.longitude,
            next_row.latitude, next_row.longitude,
        )
        speed_mps = dist_m / total_seconds
        row.state_primary = "MOVE" if speed_mps >= MOVE_SPEED_THRESHOLD_MPS else "STOP"

        row.save()
        filled += 1

    return filled
