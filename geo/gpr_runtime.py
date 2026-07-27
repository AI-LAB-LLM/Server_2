import os
import math
import numpy as np
import pandas as pd


WINDOW_SIZE = 8

RAW_LAT_COL = "Raw_Latitude"
RAW_LON_COL = "Raw_longitude"
GPS_QUALITY_COL = "gps_quality"
GPS_DECISION_COL = "gps_filter_decision"
USE_RAW_FOR_GPR_COL = "use_raw_for_gpr"
INTERP_METHOD_COL = "interp_method"

JUMP_HARD_REJECT_DIST_M = 1000.0
JUMP_HARD_REJECT_SPEED_KMPH = 120.0
JUMP_SUSPECT_DIST_M = 500.0
JUMP_SUSPECT_SPEED_KMPH = 80.0
JUMP_ACTIVE_CLUSTER_RADIUS_M = 1500.0
JUMP_RETURN_MAX_SPEED_KMPH = 80.0

# prev-current-next 세 점 기준 방향 반전 spike 제거 기준
REVERSE_SPIKE_MIN_ANGLE_DEG = 135.0
REVERSE_SPIKE_MIN_SIDE_DIST_M = 800.0
REVERSE_SPIKE_MAX_GAP_MIN = 15.0
REVERSE_SPIKE_MAX_DIRECT_SPEED_KMPH = 80.0
REVERSE_SPIKE_MIN_DETOUR_RATIO = 1.5
REVERSE_SPIKE_MIN_LINEAR_ERROR_M = 700.0

# 주변 5점 기준 single spike 제거 기준
# 22:11처럼 특정 1개 점을 제거했을 때 앞뒤 흐름의 최대 속도가 크게 줄어드는 경우만 제거한다.
CONTEXT_SPIKE_MIN_SIDE_DIST_M = 800.0
CONTEXT_SPIKE_MAX_GAP_MIN = 15.0
CONTEXT_SPIKE_MAX_BRIDGE_SPEED_KMPH = 80.0
CONTEXT_SPIKE_MAX_AFTER_REMOVAL_SPEED_KMPH = 80.0
CONTEXT_SPIKE_MIN_SPEED_DROP_KMPH = 10.0
CONTEXT_SPIKE_MIN_DETOUR_RATIO = 1.3
CONTEXT_SPIKE_MIN_LINEAR_ERROR_M = 700.0
CONTEXT_SPIKE_LOCAL_RADIUS = 2

STOPPAGE_THRESHOLD_SECONDS =20 * 60
LOCATION_EPSILON_METERS = 60.0
MOVE_SPEED_THRESHOLD_MPS = 0.5

# =========================================================
# 기본 유틸
# =========================================================
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    lat1, lon1, lat2, lon2 = map(
        math.radians,
        [float(lat1), float(lon1), float(lat2), float(lon2)]
    )
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(
        math.radians,
        [float(lat1), float(lon1), float(lat2), float(lon2)]
    )
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = (
        math.cos(lat1) * math.sin(lat2)
        - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    )
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def normalize_input_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    elif "Datetime" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Datetime"], errors="coerce")
    else:
        raise ValueError("Timestamp 또는 Datetime 컬럼이 필요합니다.")

    if "longitude" not in df.columns:
        if "Longitude" in df.columns:
            df = df.rename(columns={"Longitude": "longitude"})
        elif "Longtitude" in df.columns:
            df = df.rename(columns={"Longtitude": "longitude"})
        else:
            raise ValueError("longitude 또는 Longitude 컬럼이 필요합니다.")

    if "Latitude" not in df.columns:
        if "latitude" in df.columns:
            df = df.rename(columns={"latitude": "Latitude"})
        else:
            raise ValueError("Latitude 컬럼이 필요합니다.")

    if "device_id" not in df.columns:
        raise ValueError("device_id 컬럼이 필요합니다.")

    df["device_id"] = df["device_id"].astype(str)
    df = df.sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    return df


def ensure_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if RAW_LAT_COL not in df.columns:
        df[RAW_LAT_COL] = df["Latitude"]
    if RAW_LON_COL not in df.columns:
        df[RAW_LON_COL] = df["longitude"]
    if GPS_QUALITY_COL not in df.columns:
        df[GPS_QUALITY_COL] = "HIGH"
    if GPS_DECISION_COL not in df.columns:
        df[GPS_DECISION_COL] = "raw_used"
    if USE_RAW_FOR_GPR_COL not in df.columns:
        df[USE_RAW_FOR_GPR_COL] = True
    if INTERP_METHOD_COL not in df.columns:
        df[INTERP_METHOD_COL] = ""

    missing = df["Latitude"].isna() | df["longitude"].isna()
    df.loc[missing, GPS_QUALITY_COL] = "MISSING"
    df.loc[missing, GPS_DECISION_COL] = "linear_fill_needed"
    df.loc[missing, USE_RAW_FOR_GPR_COL] = False
    return df


def mark_bad_for_gpr_input(df: pd.DataFrame, idx, method="jump_outlier") -> None:
    # 원본 Raw_*는 보존하고, 선형보간 대상 working 좌표만 NaN 처리
    df.loc[idx, GPS_QUALITY_COL] = "BAD"
    df.loc[idx, GPS_DECISION_COL] = "linear_fill_needed"
    df.loc[idx, USE_RAW_FOR_GPR_COL] = False
    df.loc[idx, INTERP_METHOD_COL] = method
    df.loc[idx, "Latitude"] = np.nan
    df.loc[idx, "longitude"] = np.nan


def angle_diff_deg(a, b):
    """
    두 bearing 각도의 차이를 0~180도로 계산한다.
    예: 10도와 350도 차이는 20도, 0도와 180도 차이는 180도.
    """
    return abs((float(a) - float(b) + 180.0) % 360.0 - 180.0)



def _speed_kmph_between_rows(df: pd.DataFrame, idx_a, idx_b) -> float:
    """
    두 row 사이 평균 속도(km/h)를 계산한다.
    좌표/시간이 없거나 시간 순서가 이상하면 NaN을 반환한다.
    """
    lat_a = df.loc[idx_a, "Latitude"]
    lon_a = df.loc[idx_a, "longitude"]
    lat_b = df.loc[idx_b, "Latitude"]
    lon_b = df.loc[idx_b, "longitude"]
    t_a = pd.to_datetime(df.loc[idx_a, "Timestamp"], errors="coerce")
    t_b = pd.to_datetime(df.loc[idx_b, "Timestamp"], errors="coerce")

    if any(pd.isna([lat_a, lon_a, lat_b, lon_b])) or pd.isna(t_a) or pd.isna(t_b):
        return np.nan

    dt_h = (t_b - t_a).total_seconds() / 3600.0
    if dt_h <= 0:
        return np.nan

    dist_m = haversine_m(lat_a, lon_a, lat_b, lon_b)
    return (dist_m / 1000.0) / dt_h


def _max_adjacent_speed_kmph(df: pd.DataFrame, local_idxs) -> float:
    """
    local_idxs를 시간 순서대로 이어 봤을 때 인접 구간의 최대 속도(km/h)를 반환한다.
    """
    speeds = []
    for a, b in zip(local_idxs, local_idxs[1:]):
        s = _speed_kmph_between_rows(df, a, b)
        if pd.notna(s):
            speeds.append(float(s))

    if not speeds:
        return np.nan

    return max(speeds)


def detect_contextual_single_spike_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    주변 흐름을 보고 single spike를 잡는다.

    중요:
    - loop 안에서 바로 Latitude/longitude를 NaN으로 만들지 않는다.
    - 먼저 원본 좌표 기준으로 후보를 전부 모은 뒤, 가장 강한 후보만 선택한다.
    - 이렇게 해야 22:11을 제거한 뒤 그 영향으로 22:21 같은 정상점이 연쇄 제거되는 문제가 생기지 않는다.
    """
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    if "is_contextual_spike_outlier" not in df.columns:
        df["is_contextual_spike_outlier"] = False
    if "is_jump_outlier" not in df.columns:
        df["is_jump_outlier"] = False
    if "contextual_spike_score" not in df.columns:
        df["contextual_spike_score"] = np.nan

    # 판단은 항상 원본 working 좌표 기준으로 한다.
    # 후보 탐색 중간에 NaN 처리된 값이 다음 후보 판단에 영향을 주면 정상점까지 연쇄 제거된다.
    base_df = df.copy()

    for device_id, group in base_df.groupby("device_id", sort=True):
        idxs = group.index.tolist()

        if len(idxs) < 4:
            continue

        candidates = []

        for k in range(1, len(idxs) - 1):
            i_prev = idxs[k - 1]
            i_curr = idxs[k]
            i_next = idxs[k + 1]

            lat_prev = base_df.loc[i_prev, "Latitude"]
            lon_prev = base_df.loc[i_prev, "longitude"]
            lat_curr = base_df.loc[i_curr, "Latitude"]
            lon_curr = base_df.loc[i_curr, "longitude"]
            lat_next = base_df.loc[i_next, "Latitude"]
            lon_next = base_df.loc[i_next, "longitude"]

            if any(pd.isna([lat_prev, lon_prev, lat_curr, lon_curr, lat_next, lon_next])):
                continue

            t_prev = pd.to_datetime(base_df.loc[i_prev, "Timestamp"], errors="coerce")
            t_curr = pd.to_datetime(base_df.loc[i_curr, "Timestamp"], errors="coerce")
            t_next = pd.to_datetime(base_df.loc[i_next, "Timestamp"], errors="coerce")

            if pd.isna(t_prev) or pd.isna(t_curr) or pd.isna(t_next):
                continue

            gap1_min = (t_curr - t_prev).total_seconds() / 60.0
            gap2_min = (t_next - t_curr).total_seconds() / 60.0
            gap_total_min = (t_next - t_prev).total_seconds() / 60.0

            if gap1_min <= 0 or gap2_min <= 0 or gap_total_min <= 0:
                continue

            if gap1_min > CONTEXT_SPIKE_MAX_GAP_MIN:
                continue
            if gap2_min > CONTEXT_SPIKE_MAX_GAP_MIN:
                continue

            dist_prev_curr = haversine_m(lat_prev, lon_prev, lat_curr, lon_curr)
            dist_curr_next = haversine_m(lat_curr, lon_curr, lat_next, lon_next)
            dist_prev_next = haversine_m(lat_prev, lon_prev, lat_next, lon_next)

            # 작은 GPS 흔들림은 제외
            if min(dist_prev_curr, dist_curr_next) < CONTEXT_SPIKE_MIN_SIDE_DIST_M:
                continue

            bridge_speed_kmph = (dist_prev_next / 1000.0) / (gap_total_min / 60.0)
            if bridge_speed_kmph > CONTEXT_SPIKE_MAX_BRIDGE_SPEED_KMPH:
                continue

            detour_ratio = (dist_prev_curr + dist_curr_next) / max(dist_prev_next, 1.0)

            alpha = (t_curr - t_prev).total_seconds() / (t_next - t_prev).total_seconds()
            alpha = min(max(alpha, 0.0), 1.0)
            expected_lat = float(lat_prev) + alpha * (float(lat_next) - float(lat_prev))
            expected_lon = float(lon_prev) + alpha * (float(lon_next) - float(lon_prev))
            linear_error_m = haversine_m(
                expected_lat,
                expected_lon,
                lat_curr,
                lon_curr,
            )

            if (
                detour_ratio < CONTEXT_SPIKE_MIN_DETOUR_RATIO
                and linear_error_m < CONTEXT_SPIKE_MIN_LINEAR_ERROR_M
            ):
                continue

            # 주변 5점 기준으로 current 제거 전/후의 최대 속도를 비교한다.
            # 단, base_df 기준으로만 계산한다. 이전 후보가 제거되었다고 가정하지 않는다.
            local_start = max(0, k - CONTEXT_SPIKE_LOCAL_RADIUS)
            local_end = min(len(idxs), k + CONTEXT_SPIKE_LOCAL_RADIUS + 1)
            local_idxs_before = idxs[local_start:local_end]
            local_idxs_after = [x for x in local_idxs_before if x != i_curr]

            max_speed_before = _max_adjacent_speed_kmph(base_df, local_idxs_before)
            max_speed_after = _max_adjacent_speed_kmph(base_df, local_idxs_after)

            if pd.isna(max_speed_before) or pd.isna(max_speed_after):
                continue

            speed_drop = max_speed_before - max_speed_after

            is_contextual_spike = (
                max_speed_after <= CONTEXT_SPIKE_MAX_AFTER_REMOVAL_SPEED_KMPH
                and speed_drop >= CONTEXT_SPIKE_MIN_SPEED_DROP_KMPH
            )

            if not is_contextual_spike:
                continue

            # 점수는 후보 충돌 시 더 확실한 spike를 고르기 위한 값이다.
            # 속도 감소량, 선형 경로 이탈량, 우회율을 함께 반영한다.
            score = (
                float(speed_drop)
                + float(linear_error_m) / 100.0
                + max(float(detour_ratio) - 1.0, 0.0) * 20.0
            )

            candidates.append({
                "idx": i_curr,
                "pos": k,
                "score": score,
                "speed_drop": float(speed_drop),
                "max_speed_before": float(max_speed_before),
                "max_speed_after": float(max_speed_after),
                "linear_error_m": float(linear_error_m),
                "detour_ratio": float(detour_ratio),
            })

        # 후보가 겹쳐 있으면 가장 강한 후보만 남긴다.
        # 예: 22:11이 실제 spike일 때, 그 주변 정상점 22:16/22:21이 연쇄로 제거되는 것을 막는다.
        candidates.sort(key=lambda x: x["score"], reverse=True)
        selected = []
        selected_positions = []

        for cand in candidates:
            if any(abs(cand["pos"] - p) <= CONTEXT_SPIKE_LOCAL_RADIUS for p in selected_positions):
                continue
            selected.append(cand)
            selected_positions.append(cand["pos"])

        for cand in selected:
            i_curr = cand["idx"]
            mark_bad_for_gpr_input(df, i_curr, method="contextual_single_spike_outlier")
            df.loc[i_curr, "is_contextual_spike_outlier"] = True
            df.loc[i_curr, "is_jump_outlier"] = True
            df.loc[i_curr, "contextual_spike_score"] = cand["score"]

    return df

def detect_reverse_spike_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    prev-current-next 세 점을 보고 current가 양옆 흐름과 반대 방향으로 튄 중간점이면 BAD 처리한다.

    잡고 싶은 케이스:
        A = 이전 정상점
        B = 현재점
        C = 다음 정상점

        A -> B 방향과 B -> C 방향이 거의 반대이고,
        A -> C로 바로 이어보면 속도가 말이 되며,
        B가 A-C 흐름에서 크게 벗어난 경우 B를 outlier로 본다.

    단순히 방향이 반대라고 제거하지 않고, 거리/시간/직선 오차/우회율을 같이 본다.
    """
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    if "is_reverse_spike_outlier" not in df.columns:
        df["is_reverse_spike_outlier"] = False
    if "is_jump_outlier" not in df.columns:
        df["is_jump_outlier"] = False

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()

        if len(idxs) < 3:
            continue

        for k in range(1, len(idxs) - 1):
            i_prev = idxs[k - 1]
            i_curr = idxs[k]
            i_next = idxs[k + 1]

            lat_prev = df.loc[i_prev, "Latitude"]
            lon_prev = df.loc[i_prev, "longitude"]
            lat_curr = df.loc[i_curr, "Latitude"]
            lon_curr = df.loc[i_curr, "longitude"]
            lat_next = df.loc[i_next, "Latitude"]
            lon_next = df.loc[i_next, "longitude"]

            if any(pd.isna([lat_prev, lon_prev, lat_curr, lon_curr, lat_next, lon_next])):
                continue

            t_prev = pd.to_datetime(df.loc[i_prev, "Timestamp"], errors="coerce")
            t_curr = pd.to_datetime(df.loc[i_curr, "Timestamp"], errors="coerce")
            t_next = pd.to_datetime(df.loc[i_next, "Timestamp"], errors="coerce")

            if pd.isna(t_prev) or pd.isna(t_curr) or pd.isna(t_next):
                continue

            gap1_min = (t_curr - t_prev).total_seconds() / 60.0
            gap2_min = (t_next - t_curr).total_seconds() / 60.0
            gap_total_min = (t_next - t_prev).total_seconds() / 60.0

            if gap1_min <= 0 or gap2_min <= 0 or gap_total_min <= 0:
                continue

            # 너무 긴 시간 간격은 실제 이동 가능성이 커서 reverse spike 판단에서 제외
            if gap1_min > REVERSE_SPIKE_MAX_GAP_MIN:
                continue
            if gap2_min > REVERSE_SPIKE_MAX_GAP_MIN:
                continue

            dist_prev_curr = haversine_m(lat_prev, lon_prev, lat_curr, lon_curr)
            dist_curr_next = haversine_m(lat_curr, lon_curr, lat_next, lon_next)
            dist_prev_next = haversine_m(lat_prev, lon_prev, lat_next, lon_next)

            # 작은 GPS 흔들림은 방향이 반대로 나와도 제거하지 않음
            if min(dist_prev_curr, dist_curr_next) < REVERSE_SPIKE_MIN_SIDE_DIST_M:
                continue

            bearing_prev_curr = calculate_bearing(lat_prev, lon_prev, lat_curr, lon_curr)
            bearing_curr_next = calculate_bearing(lat_curr, lon_curr, lat_next, lon_next)
            reverse_angle = angle_diff_deg(bearing_prev_curr, bearing_curr_next)

            # A->B와 B->C가 충분히 반대 방향이 아니면 제외
            if reverse_angle < REVERSE_SPIKE_MIN_ANGLE_DEG:
                continue

            # A->C로 바로 이동했다고 보면 정상적인 속도인지 확인
            direct_speed_kmph = (dist_prev_next / 1000.0) / (gap_total_min / 60.0)
            if direct_speed_kmph > REVERSE_SPIKE_MAX_DIRECT_SPEED_KMPH:
                continue

            # B를 거쳐 가면 얼마나 비정상적으로 돌아가는지 확인
            detour_ratio = (dist_prev_curr + dist_curr_next) / max(dist_prev_next, 1.0)

            # B 시점에 A-C 선형 흐름상 있어야 할 위치와 실제 B의 거리
            alpha = (t_curr - t_prev).total_seconds() / (t_next - t_prev).total_seconds()
            alpha = min(max(alpha, 0.0), 1.0)

            expected_lat = float(lat_prev) + alpha * (float(lat_next) - float(lat_prev))
            expected_lon = float(lon_prev) + alpha * (float(lon_next) - float(lon_prev))

            linear_error_m = haversine_m(
                expected_lat,
                expected_lon,
                lat_curr,
                lon_curr,
            )

            is_reverse_spike = (
                detour_ratio >= REVERSE_SPIKE_MIN_DETOUR_RATIO
                or linear_error_m >= REVERSE_SPIKE_MIN_LINEAR_ERROR_M
            )

            if not is_reverse_spike:
                continue

            mark_bad_for_gpr_input(df, i_curr, method="reverse_spike_outlier")
            df.loc[i_curr, "is_reverse_spike_outlier"] = True
            df.loc[i_curr, "is_jump_outlier"] = True

    return df


# =========================================================
# jump/outlier 처리
# =========================================================
def detect_and_tag_jump_outliers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    if "is_jump_outlier" not in df.columns:
        df["is_jump_outlier"] = False
    if "is_jump_suspect" not in df.columns:
        df["is_jump_suspect"] = False

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()
        last_good_idx = None
        active_cluster_center = None

        for idx in idxs:
            lat_cur = df.loc[idx, "Latitude"]
            lon_cur = df.loc[idx, "longitude"]
            t_cur = pd.to_datetime(df.loc[idx, "Timestamp"], errors="coerce")

            if pd.isna(lat_cur) or pd.isna(lon_cur) or pd.isna(t_cur):
                continue

            if last_good_idx is None:
                last_good_idx = idx
                continue

            lat_last = df.loc[last_good_idx, "Latitude"]
            lon_last = df.loc[last_good_idx, "longitude"]
            t_last = pd.to_datetime(df.loc[last_good_idx, "Timestamp"], errors="coerce")

            if pd.isna(lat_last) or pd.isna(lon_last) or pd.isna(t_last):
                last_good_idx = idx
                active_cluster_center = None
                continue

            dt_h = (t_cur - t_last).total_seconds() / 3600.0
            if dt_h <= 0:
                continue

            dist_m = haversine_m(lat_last, lon_last, lat_cur, lon_cur)
            speed_kmph = (dist_m / 1000.0) / dt_h

            if active_cluster_center is not None:
                dist_to_bad_cluster = haversine_m(
                    lat_cur,
                    lon_cur,
                    active_cluster_center[0],
                    active_cluster_center[1],
                )

                if dist_to_bad_cluster <= JUMP_ACTIVE_CLUSTER_RADIUS_M:
                    mark_bad_for_gpr_input(df, idx, method="jump_outlier")
                    df.loc[idx, "is_jump_outlier"] = True
                    continue

                if speed_kmph <= JUMP_RETURN_MAX_SPEED_KMPH:
                    df.loc[idx, GPS_QUALITY_COL] = "HIGH"
                    df.loc[idx, GPS_DECISION_COL] = "raw_used_return_from_jump"
                    df.loc[idx, USE_RAW_FOR_GPR_COL] = True
                    last_good_idx = idx
                    active_cluster_center = None
                    continue

            is_hard_bad = (
                dist_m >= JUMP_HARD_REJECT_DIST_M
                and speed_kmph >= JUMP_HARD_REJECT_SPEED_KMPH
            )
            is_suspect = (
                dist_m >= JUMP_SUSPECT_DIST_M
                and speed_kmph >= JUMP_SUSPECT_SPEED_KMPH
            )

            if is_hard_bad:
                active_cluster_center = (float(lat_cur), float(lon_cur))
                mark_bad_for_gpr_input(df, idx, method="jump_outlier")
                df.loc[idx, "is_jump_outlier"] = True
                continue

            if is_suspect:
                df.loc[idx, GPS_QUALITY_COL] = "LOW"
                df.loc[idx, GPS_DECISION_COL] = "suspect_kept"
                df.loc[idx, USE_RAW_FOR_GPR_COL] = True
                df.loc[idx, "is_jump_suspect"] = True
                if not str(df.loc[idx, INTERP_METHOD_COL]).strip():
                    df.loc[idx, INTERP_METHOD_COL] = "jump_suspect_kept"

            last_good_idx = idx

    return df


# =========================================================
# stale 처리
# =========================================================
def detect_and_fix_stale_gps_linear(
    df: pd.DataFrame,
    dist_same_m=1.0,
    dist_jump_m=1000.0,
    min_gap_min=5.0,
    post_check_n=3,
) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    if "is_stale" not in df.columns:
        df["is_stale"] = False

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()
        if len(idxs) < 3:
            continue

        for k in range(1, len(idxs) - 1):
            i_prev = idxs[k - 1]
            i_curr = idxs[k]

            lat_prev = df.loc[i_prev, "Latitude"]
            lon_prev = df.loc[i_prev, "longitude"]
            lat_curr = df.loc[i_curr, "Latitude"]
            lon_curr = df.loc[i_curr, "longitude"]

            if any(pd.isna([lat_prev, lon_prev, lat_curr, lon_curr])):
                continue

            dist_prev = haversine_m(lat_prev, lon_prev, lat_curr, lon_curr)
            if dist_prev > dist_same_m:
                continue

            t_prev = pd.to_datetime(df.loc[i_prev, "Timestamp"], errors="coerce")
            t_curr = pd.to_datetime(df.loc[i_curr, "Timestamp"], errors="coerce")
            if pd.isna(t_prev) or pd.isna(t_curr):
                continue

            gap_min = (t_curr - t_prev).total_seconds() / 60.0
            if gap_min < min_gap_min:
                continue

            i_next_jump = None
            for j in range(1, post_check_n + 1):
                if k + j >= len(idxs):
                    break
                i_next = idxs[k + j]
                lat_next = df.loc[i_next, "Latitude"]
                lon_next = df.loc[i_next, "longitude"]
                if pd.isna(lat_next) or pd.isna(lon_next):
                    continue
                dist_next = haversine_m(lat_curr, lon_curr, lat_next, lon_next)
                if dist_next >= dist_jump_m:
                    i_next_jump = i_next
                    break

            if i_next_jump is None:
                continue

            t_next = pd.to_datetime(df.loc[i_next_jump, "Timestamp"], errors="coerce")
            if pd.isna(t_next) or t_next <= t_prev:
                new_lat = float(lat_prev)
                new_lon = float(lon_prev)
            else:
                total = (t_next - t_prev).total_seconds()
                alpha = (t_curr - t_prev).total_seconds() / total
                alpha = min(max(alpha, 0.0), 1.0)

                lat_next = float(df.loc[i_next_jump, "Latitude"])
                lon_next = float(df.loc[i_next_jump, "longitude"])
                new_lat = float(lat_prev) + alpha * (lat_next - float(lat_prev))
                new_lon = float(lon_prev) + alpha * (lon_next - float(lon_prev))

            df.loc[i_curr, "Latitude"] = new_lat
            df.loc[i_curr, "longitude"] = new_lon
            df.loc[i_curr, "is_stale"] = True
            df.loc[i_curr, INTERP_METHOD_COL] = "stale_linear"
            df.loc[i_curr, GPS_QUALITY_COL] = "LOW"
            df.loc[i_curr, GPS_DECISION_COL] = "corrected_stale_linear"
            df.loc[i_curr, USE_RAW_FOR_GPR_COL] = False

    return df


def fill_missing_gps_linear_between_valid_points(
    df: pd.DataFrame,
    max_gap_minutes: float = 60.0,
    max_gap_rows: int = 12,
    max_bridge_speed_kmph: float = 120.0,
    decision_value: str = "linear_filled_fallback",
    method_value: str = "linear_initial_or_short_window",
) -> pd.DataFrame:
    """
    앞뒤 정상 좌표로 채울 수 있는 NULL 결측을 마지막 fallback으로 선형보간
    """
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    if "is_missing_linear_filled" not in df.columns:
        df["is_missing_linear_filled"] = False

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()
        n = len(idxs)
        k = 0

        while k < n:
            idx = idxs[k]
            lat = df.loc[idx, "Latitude"]
            lon = df.loc[idx, "longitude"]

            # 현재 row가 결측이 아니면 다음 row로 이동
            if not (pd.isna(lat) or pd.isna(lon)):
                k += 1
                continue

            # 연속 결측 block 탐색
            block_start_pos = k
            while k < n:
                cur_idx = idxs[k]
                lat_cur = df.loc[cur_idx, "Latitude"]
                lon_cur = df.loc[cur_idx, "longitude"]
                if not (pd.isna(lat_cur) or pd.isna(lon_cur)):
                    break
                k += 1
            block_end_pos = k - 1

            prev_pos = block_start_pos - 1
            next_pos = k

            # 앞 정상점과 뒤 정상점이 모두 있어야 선형보간 가능
            if prev_pos < 0 or next_pos >= n:
                continue

            prev_idx = idxs[prev_pos]
            next_idx = idxs[next_pos]

            lat_prev = df.loc[prev_idx, "Latitude"]
            lon_prev = df.loc[prev_idx, "longitude"]
            lat_next = df.loc[next_idx, "Latitude"]
            lon_next = df.loc[next_idx, "longitude"]

            if any(pd.isna([lat_prev, lon_prev, lat_next, lon_next])):
                continue

            t_prev = pd.to_datetime(df.loc[prev_idx, "Timestamp"], errors="coerce")
            t_next = pd.to_datetime(df.loc[next_idx, "Timestamp"], errors="coerce")
            if pd.isna(t_prev) or pd.isna(t_next) or t_next <= t_prev:
                continue

            gap_minutes = (t_next - t_prev).total_seconds() / 60.0
            block_len = block_end_pos - block_start_pos + 1

            if gap_minutes > max_gap_minutes:
                continue
            if block_len > max_gap_rows:
                continue

            bridge_dist_m = haversine_m(
                float(lat_prev), float(lon_prev),
                float(lat_next), float(lon_next),
            )
            bridge_speed_kmph = (bridge_dist_m / 1000.0) / (gap_minutes / 60.0)
            if bridge_speed_kmph > max_bridge_speed_kmph:
                continue

            total_seconds = (t_next - t_prev).total_seconds()

            for fill_pos in range(block_start_pos, block_end_pos + 1):
                fill_idx = idxs[fill_pos]
                t_cur = pd.to_datetime(df.loc[fill_idx, "Timestamp"], errors="coerce")
                if pd.isna(t_cur):
                    continue

                alpha = (t_cur - t_prev).total_seconds() / total_seconds
                alpha = min(max(alpha, 0.0), 1.0)

                new_lat = float(lat_prev) + alpha * (float(lat_next) - float(lat_prev))
                new_lon = float(lon_prev) + alpha * (float(lon_next) - float(lon_prev))

                df.loc[fill_idx, "Latitude"] = new_lat
                df.loc[fill_idx, "longitude"] = new_lon
                df.loc[fill_idx, GPS_QUALITY_COL] = "LOW"
                df.loc[fill_idx, GPS_DECISION_COL] = decision_value
                df.loc[fill_idx, USE_RAW_FOR_GPR_COL] = False
                df.loc[fill_idx, INTERP_METHOD_COL] = method_value
                df.loc[fill_idx, "is_missing_linear_filled"] = True

    return df

def restore_unfilled_rows_with_raw(
    df: pd.DataFrame,
    decision_value: str = "raw_restored_after_failed_fill",
    method_suffix: str = "raw_fallback",
) -> pd.DataFrame:
    """
    선형보간으로도 끝까지 채우지 못한 row는
    Raw_Latitude / Raw_longitude를 최종 Latitude / longitude로 복구한다.

    단, 이 값은 outlier로 의심된 raw를 다시 쓰는 것이므로
    gps_quality는 LOW로 두고, use_raw_for_gpr는 False로 유지한다.
    """
    df = df.copy()
    df = ensure_quality_columns(df)

    if "is_raw_restored_fallback" not in df.columns:
        df["is_raw_restored_fallback"] = False

    need_restore = (
        (
            df["Latitude"].isna()
            | df["longitude"].isna()
            | df[GPS_DECISION_COL].astype(str).str.contains("fill_needed", na=False)
        )
        & df[RAW_LAT_COL].notna()
        & df[RAW_LON_COL].notna()
    )

    df.loc[need_restore, "Latitude"] = df.loc[need_restore, RAW_LAT_COL]
    df.loc[need_restore, "longitude"] = df.loc[need_restore, RAW_LON_COL]

    df.loc[need_restore, GPS_QUALITY_COL] = "LOW"
    df.loc[need_restore, GPS_DECISION_COL] = decision_value

    # 최종 좌표로는 raw를 복구하지만,
    # 이후 보정 입력에 신뢰 좌표로 쓰지는 않겠다는 의미
    df.loc[need_restore, USE_RAW_FOR_GPR_COL] = False

    old_method = df.loc[need_restore, INTERP_METHOD_COL].astype(str)

    df.loc[need_restore, INTERP_METHOD_COL] = np.where(
        old_method.str.strip().ne(""),
        old_method + "|" + method_suffix,
        method_suffix,
    )

    df.loc[need_restore, "is_raw_restored_fallback"] = True

    return df

# =========================================================
# feature 생성
# =========================================================
def add_relative_time_feature(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df["stoppage_duration_at_point"] = 0.0
    df["trip_id"] = 0
    df["relative_time"] = 0.0

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()
        trip_id = 1
        rel_time = 0.0
        stop_duration = 0.0

        for pos, idx in enumerate(idxs):
            if pos == 0:
                df.loc[idx, "trip_id"] = trip_id
                continue

            prev_idx = idxs[pos - 1]
            t_prev = pd.to_datetime(df.loc[prev_idx, "Timestamp"])
            t_cur = pd.to_datetime(df.loc[idx, "Timestamp"])
            dt = max((t_cur - t_prev).total_seconds(), 0.0)

            lat_prev = df.loc[prev_idx, "Latitude"]
            lon_prev = df.loc[prev_idx, "longitude"]
            lat_cur = df.loc[idx, "Latitude"]
            lon_cur = df.loc[idx, "longitude"]

            same_place = False
            if not any(pd.isna([lat_prev, lon_prev, lat_cur, lon_cur])):
                same_place = haversine_m(lat_prev, lon_prev, lat_cur, lon_cur) <= LOCATION_EPSILON_METERS

            if same_place:
                stop_duration += dt
            else:
                stop_duration = 0.0
                rel_time += dt

            df.loc[idx, "trip_id"] = trip_id
            df.loc[idx, "relative_time"] = rel_time
            df.loc[idx, "stoppage_duration_at_point"] = stop_duration

    return df


def add_motion_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df["cumulative_distance_km"] = 0.0
    df["velocity_kmph"] = 0.0
    df["bearing"] = 0.0

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()
        cum = 0.0
        prev_bearing = 0.0

        for k in range(1, len(idxs)):
            prev_idx = idxs[k - 1]
            cur_idx = idxs[k]

            lat1 = df.loc[prev_idx, "Latitude"]
            lon1 = df.loc[prev_idx, "longitude"]
            lat2 = df.loc[cur_idx, "Latitude"]
            lon2 = df.loc[cur_idx, "longitude"]

            if any(pd.isna([lat1, lon1, lat2, lon2])):
                df.loc[cur_idx, "bearing"] = prev_bearing
                continue

            dt = (pd.to_datetime(df.loc[cur_idx, "Timestamp"]) - pd.to_datetime(df.loc[prev_idx, "Timestamp"])).total_seconds()
            dist_m = haversine_m(lat1, lon1, lat2, lon2)
            dist_km = dist_m / 1000.0
            cum += dist_km

            df.loc[cur_idx, "cumulative_distance_km"] = cum
            df.loc[cur_idx, "velocity_kmph"] = dist_km / dt * 3600 if dt > 0 else 0.0

            if dist_m > 0.1:
                prev_bearing = calculate_bearing(lat1, lon1, lat2, lon2)
            df.loc[cur_idx, "bearing"] = prev_bearing

    return df


def add_trip_pos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df["trip_pos"] = df.groupby(["device_id", "trip_id"]).cumcount()
    return df


def recompute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = add_relative_time_feature(df)
    df = add_motion_features(df)
    df = add_trip_pos(df)
    return df


# =========================================================
# 선형보간 전용 결측 보정
# =========================================================
def _is_valid_coord_row(df: pd.DataFrame, idx) -> bool:
    t = pd.to_datetime(df.loc[idx, "Timestamp"], errors="coerce")
    return (
        pd.notna(df.loc[idx, "Latitude"])
        and pd.notna(df.loc[idx, "longitude"])
        and pd.notna(t)
    )


def _find_prev_next_valid_coord(
    df: pd.DataFrame,
    idxs,
    pos: int,
):
    prev_idx = None
    next_idx = None

    for j in range(pos - 1, -1, -1):
        cand = idxs[j]
        if _is_valid_coord_row(df, cand):
            prev_idx = cand
            break

    for j in range(pos + 1, len(idxs)):
        cand = idxs[j]
        if _is_valid_coord_row(df, cand):
            next_idx = cand
            break

    return prev_idx, next_idx


def _linear_fill_single_between(
    df: pd.DataFrame,
    fill_idx,
    prev_idx,
    next_idx,
    max_gap_minutes: float = 60.0,
    max_bridge_speed_kmph: float = 120.0,
    method: str = "linear_initial_or_short_window",
) -> bool:
    """
    fill_idx 한 row를 prev_idx~next_idx 사이 시간 비율로 선형보간한다.
    성공하면 True, 아니면 False.
    """
    if prev_idx is None or next_idx is None:
        return False

    if not (_is_valid_coord_row(df, prev_idx) and _is_valid_coord_row(df, next_idx)):
        return False

    t_prev = pd.to_datetime(df.loc[prev_idx, "Timestamp"], errors="coerce")
    t_cur = pd.to_datetime(df.loc[fill_idx, "Timestamp"], errors="coerce")
    t_next = pd.to_datetime(df.loc[next_idx, "Timestamp"], errors="coerce")

    if pd.isna(t_prev) or pd.isna(t_cur) or pd.isna(t_next):
        return False
    if not (t_prev < t_cur < t_next):
        return False

    gap_minutes = (t_next - t_prev).total_seconds() / 60.0
    if gap_minutes <= 0 or gap_minutes > max_gap_minutes:
        return False

    lat_prev = float(df.loc[prev_idx, "Latitude"])
    lon_prev = float(df.loc[prev_idx, "longitude"])
    lat_next = float(df.loc[next_idx, "Latitude"])
    lon_next = float(df.loc[next_idx, "longitude"])

    bridge_dist_m = haversine_m(lat_prev, lon_prev, lat_next, lon_next)
    bridge_speed_kmph = (bridge_dist_m / 1000.0) / (gap_minutes / 60.0)
    if bridge_speed_kmph > max_bridge_speed_kmph:
        return False

    total_seconds = (t_next - t_prev).total_seconds()
    alpha = (t_cur - t_prev).total_seconds() / total_seconds
    alpha = min(max(alpha, 0.0), 1.0)

    new_lat = lat_prev + alpha * (lat_next - lat_prev)
    new_lon = lon_prev + alpha * (lon_next - lon_prev)

    df.loc[fill_idx, "Latitude"] = float(new_lat)
    df.loc[fill_idx, "longitude"] = float(new_lon)
    df.loc[fill_idx, GPS_QUALITY_COL] = "LOW"
    df.loc[fill_idx, GPS_DECISION_COL] = "linear_filled_short_window"
    df.loc[fill_idx, USE_RAW_FOR_GPR_COL] = False
    df.loc[fill_idx, INTERP_METHOD_COL] = method

    if "is_missing_linear_filled" not in df.columns:
        df["is_missing_linear_filled"] = False
    df.loc[fill_idx, "is_missing_linear_filled"] = True

    return True


def linear_fill_missing_autoregressive_replacement(
    df: pd.DataFrame,
    linear_max_gap_minutes: float = 60.0,
    linear_max_bridge_speed_kmph: float = 120.0,
) -> pd.DataFrame:
    """
    기존 autoregressive 보정 구간을 대체하는 선형보간 전용 결측 보정 함수.

    처리 방식:
    1) Latitude/longitude가 NaN이거나 *_fill_needed 상태인 row를 찾는다.
    2) 같은 device_id 안에서 현재 row 앞/뒤의 정상 좌표를 찾는다.
    3) 앞 정상점~뒤 정상점 사이를 Timestamp 비율(alpha)로 선형보간한다.
    4) 보간된 좌표를 즉시 Latitude/longitude에 반영한다.
       따라서 연속 결측 block도 앞에서부터 순차적으로 채워질 수 있다.
    5) 앞/뒤 anchor가 없거나 gap/speed 조건을 넘으면 NaN으로 남기고,
       이후 restore_unfilled_rows_with_raw()에서 raw fallback 여부를 결정한다.
    """
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    df = ensure_quality_columns(df)

    for col, default in [
        ("linear_fill_attempted", False),
        ("linear_fill_skip_reason", ""),
    ]:
        if col not in df.columns:
            df[col] = default

    # 최초 feature/state 계산
    df = recompute_features(df)
    df = detect_stop_move_primary(df)

    for device_id, group in df.groupby("device_id", sort=True):
        idxs = group.index.tolist()

        for pos, idx in enumerate(idxs):
            decision = str(df.loc[idx, GPS_DECISION_COL])
            needs_fill = (
                pd.isna(df.loc[idx, "Latitude"])
                or pd.isna(df.loc[idx, "longitude"])
                or "fill_needed" in decision
            )

            if not needs_fill:
                continue

            df.loc[idx, "linear_fill_attempted"] = True

            prev_idx, next_idx = _find_prev_next_valid_coord(df, idxs, pos)
            filled_linear = _linear_fill_single_between(
                df,
                fill_idx=idx,
                prev_idx=prev_idx,
                next_idx=next_idx,
                max_gap_minutes=linear_max_gap_minutes,
                max_bridge_speed_kmph=linear_max_bridge_speed_kmph,
                method="linear_autoregressive_replacement",
            )

            if filled_linear:
                df.loc[idx, GPS_DECISION_COL] = "linear_autoregressive_replacement_filled"
                df.loc[idx, "linear_fill_skip_reason"] = ""

                # 다음 결측 row가 방금 선형보간된 좌표를 앞 anchor로 쓸 수 있게 재계산
                df = recompute_features(df)
                df = detect_stop_move_primary(df)
            else:
                if prev_idx is None and next_idx is None:
                    reason = "no_prev_next_valid_anchor"
                elif prev_idx is None:
                    reason = "no_prev_valid_anchor"
                elif next_idx is None:
                    reason = "no_next_valid_anchor"
                else:
                    reason = "linear_gap_or_speed_rejected"
                df.loc[idx, "linear_fill_skip_reason"] = reason

    return df


# 기존 함수명을 다른 파일에서 import하고 있을 수 있으므로 호환용 wrapper로 남긴다. 실제 내부 처리는 linear만 수행한다.
def gpr_fill_missing_hybrid_autoregressive(
    df: pd.DataFrame,
    gpr_lat=None,
    gpr_lon=None,
    scaler_X=None,
    scaler_y_lat=None,
    scaler_y_lon=None,
    window_size: int = WINDOW_SIZE,
    min_move_points: int = 0,
    max_uncertainty_m=None,
    linear_max_gap_minutes: float = 60.0,
    linear_max_bridge_speed_kmph: float = 120.0,
) -> pd.DataFrame:
    return linear_fill_missing_autoregressive_replacement(
        df=df,
        linear_max_gap_minutes=linear_max_gap_minutes,
        linear_max_bridge_speed_kmph=linear_max_bridge_speed_kmph,
    )


# =========================================================
# STOP/MOVE 판정
# =========================================================
def detect_stop_move_primary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["device_id", "Timestamp"]).reset_index(drop=True)
    if "velocity_kmph" not in df.columns:
        df = add_motion_features(df)

    # 아직 최종 좌표가 없는 row를 STOP으로 오판하지 않기 위해 UNKNOWN으로 둔다.
    df["state_primary"] = "UNKNOWN"

    for idx in df.index:
        if pd.isna(df.loc[idx, "Latitude"]) or pd.isna(df.loc[idx, "longitude"]):
            df.loc[idx, "state_primary"] = "UNKNOWN"
            continue

        v = pd.to_numeric(df.loc[idx, "velocity_kmph"], errors="coerce")
        if pd.isna(v):
            df.loc[idx, "state_primary"] = "UNKNOWN"
            continue

        v_mps = float(v) / 3.6
        df.loc[idx, "state_primary"] = "MOVE" if v_mps >= MOVE_SPEED_THRESHOLD_MPS else "STOP"

    return df


# =========================================================
# Runtime class
# =========================================================
class GPRRuntime:
    def __init__(self, model_dir: str = "", version: str = "", device_id: str = ""):
        """
        모델 파일을 사용하지 않는 linear-only runtime.

        기존 서버 코드가 GPRRuntime(model_dir, version, device_id) 형태로 호출할 수 있으므로
        클래스명과 인자는 그대로 유지하지만, bundle은 로드하지 않는다.
        """
        self.model_dir = model_dir
        self.version = version
        self.device_id = str(device_id)
        self.bundle_path = None
        self.bundle = None
        self.long_gap_gpr = None  # legacy attribute
        self.anchor_zones = None
        self.window_size = WINDOW_SIZE

    def preprocess_and_predict(self, recent_df: pd.DataFrame) -> pd.DataFrame:
        """
        recent_df:
            서버 DB에서 조회한 최근 데이터.
            5분 간격 기준 최근 20분 조회 권장.

        return:
            보정된 Latitude / longitude와 state_primary가 포함된 DataFrame.
        """
        df = normalize_input_columns(recent_df)
        df = ensure_quality_columns(df)

        # 1. 주변 5점 기준으로 single spike를 먼저 제거
        #    22:11처럼 특정 한 점을 제거했을 때 주변 흐름이 자연스러워지는 경우를 먼저 BAD 처리한다.
        #    이 처리를 먼저 해야 22:06 같은 직전 정상점이 reverse spike로 오판되는 것을 줄일 수 있다.
        df = detect_contextual_single_spike_outliers(df)

        # 2. prev-current-next 기준 방향 반전 spike 제거
        #    이미 contextual spike로 제거된 row가 있으면 해당 row는 NaN이므로 자동으로 건너뛴다.
        df = detect_reverse_spike_outliers(df)

        # 3. jump/outlier 좌표를 선형보간 대상으로 제외
        df = detect_and_tag_jump_outliers(df)

        # 4. stale GPS는 가능한 경우 선형보간 교정
        df = detect_and_fix_stale_gps_linear(df)

        # 5. feature 생성
        df = recompute_features(df)

        # 6. 기존 autoregressive 보정 구간을 선형보간으로 대체
        df = linear_fill_missing_autoregressive_replacement(
            df=df,
            linear_max_gap_minutes=20.0,
            linear_max_bridge_speed_kmph=120.0,
        )

        df = fill_missing_gps_linear_between_valid_points(
            df,
            max_gap_minutes=20.0,
            max_gap_rows=12,
            max_bridge_speed_kmph=120.0,
            decision_value="linear_filled_final_fallback",
            method_value="linear_final_fallback",
        )

        # 6. 최종 좌표 기준 feature/state 재계산
        df = recompute_features(df)
        df = detect_stop_move_primary(df)

        # 7. 참고 예측 및 predicted_* 적용 단계 제거
        #    이 버전은 최종 좌표 보정을 linear + raw fallback으로만 수행한다.

        # 8. 선형보간으로도 못 채운 row는 Raw 좌표를 최종 좌표로 복구
        df = restore_unfilled_rows_with_raw(
            df,
            decision_value="raw_restored_after_failed_fill",
            method_suffix="raw_fallback",
        )

        # 10. 최종 좌표 기준 feature/state 재계산
        df = recompute_features(df)
        df = detect_stop_move_primary(df)

        # 10. 최종 출력용 컬럼명 정리
        # 내부 계산용 longitude는 유지하고,
        # 저장/출력용 Longitude 컬럼을 따로 만든다.
        if "longitude" in df.columns:
            df["Longitude"] = df["longitude"]

        if RAW_LON_COL in df.columns:
            df["Raw_Longitude"] = df[RAW_LON_COL]

        return df