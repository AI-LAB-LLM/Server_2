from typing import Dict, Any, List, Optional, Sequence
import numpy as np
import heartpy as hp
import logging
import os

access_log = logging.getLogger("django.request")
access_log.error(f"[WEAR_PROBE] wear_runtime LOADED file={__file__} pid={os.getpid()}")

SAMPLE_RATE = 25
TYPICAL_STD = 4000
THRESHOLD_LOW = TYPICAL_STD * 0.2      # 800
THRESHOLD_HIGH = TYPICAL_STD * 25      # 100000
SUBCHUNK_COUNT = 5

# ===== Motion (워치 앱 PPGService.MotionLabeler와 동일한 기준) =====
STANDARD_GRAVITY = 9.80665     # android SensorManager.STANDARD_GRAVITY
MOTION_WINDOW_MS = 1000        # windowMs
MOTION_MIN_SAMPLES = 3         # minSamples → 미만이면 "no_imu"
MOTION_DEV_THR = 0.04          # |mean(svm/g) - 1| < 0.04
MOTION_STD_THR = 0.025         # std(svm/g)       < 0.025 (모집단 표준편차)
MOTION_MAD_THR = 0.02          # MAD(svm/g)*1.4826 < 0.02
IMU_SAMPLE_RATE = 25           # 서버로 들어오는 IMU 샘플링 레이트 (12초 청크 = 300샘플 = 1초 창 12개)
IMU_UNIT = "auto"              # "mps2" | "g" | "auto" (svm 중앙값 < 3 이면 g 단위로 간주)

# ===== Motion → 청크 단위 invalid 판정 (워치 앱에는 없던, 서버에서 새로 정하는 값) =====
MOTION_RATIO_THR = 0.3         # 라벨이 나온 1초 창 중 motion 비율이 이 값 이상이면 invalid (12창 기준 4창 이상)
MOTION_MIN_WINDOWS = 6         # stable/motion 라벨이 나온 창이 이보다 적으면(12초 중 절반 미만) motion 판정 생략


def _preprocess_green(values: list | np.ndarray, sr: int) -> np.ndarray:
    """레거시 임계치에 영향 최소화: float화, NaN/Inf 정리, 약한 스파이크 클립, DC 제거."""
    x = np.asarray(values, dtype=float).ravel()
    if x.size == 0:
        return x

    finite = np.isfinite(x)
    if not finite.any():
        return np.array([], dtype=float)  # 전부 NaN/Inf면 invalid로 처리하게 빈 배열 반환
    med = np.median(x[finite])
    x[~finite] = med

    # 스파이크 억제
    mad = np.median(np.abs(x - med)) or 1e-9
    lo = med - 8.0 * mad
    hi = med + 8.0 * mad
    x = np.clip(x, lo, hi)

    # DC 제거
    x = x - np.median(x)

    return x


# -------------------- Motion --------------------
def _to_svm(imu: Sequence, unit: str = IMU_UNIT) -> np.ndarray:
    """(N,3) [ax,ay,az] 또는 (N,) svm 입력을 svm(m/s²) 1D 배열로 변환."""
    a = np.asarray(imu, dtype=float)
    if a.ndim == 2 and a.shape[1] >= 3:
        a = np.sqrt(a[:, 0] ** 2 + a[:, 1] ** 2 + a[:, 2] ** 2)
    a = a.ravel()
    a = a[np.isfinite(a)] if a.size else a
    if a.size and (unit == "g" or (unit == "auto" and np.median(a) < 3.0)):
        a = a * STANDARD_GRAVITY  # g 단위 입력 → m/s²
    return a


def _motion_label(svm_window: np.ndarray) -> str:
    """Kotlin MotionLabeler.getLabel()의 1:1 포팅."""
    n = svm_window.size
    if n < MOTION_MIN_SAMPLES:
        return "no_imu"
    gn = svm_window / STANDARD_GRAVITY
    mean = float(gn.mean())
    dev_from_1g = abs(mean - 1.0)
    std = float(np.sqrt(np.mean((gn - mean) ** 2)))          # 모집단 std (Kotlin: / size)
    s = np.sort(gn)
    med = float(s[n // 2])                                   # Kotlin: sorted[size/2] (짝수면 상위 중앙값)
    mad = float(np.sort(np.abs(s - med))[n // 2]) * 1.4826
    stable = (dev_from_1g < MOTION_DEV_THR) and (std < MOTION_STD_THR) and (mad < MOTION_MAD_THR)
    return "stable" if stable else "motion"


def _split_windows(svm: np.ndarray, ts_ms: Optional[Sequence], imu_sr: int) -> List[np.ndarray]:
    """1초 창으로 분할. ts_ms가 있으면 시간 기준 [t0+k*1000, t0+(k+1)*1000), 없으면 imu_sr 샘플 단위."""
    if ts_ms is not None:
        t = np.asarray(ts_ms, dtype=float).ravel()
        if t.size != svm.size:
            raise ValueError(f"imu_ts_ms length {t.size} != imu length {svm.size}")
        if t.size == 0:
            return []
        idx = ((t - t.min()) // MOTION_WINDOW_MS).astype(int)
        return [svm[idx == k] for k in range(int(idx.max()) + 1)]
    step = max(int(imu_sr * MOTION_WINDOW_MS / 1000), 1)
    return [svm[i:i + step] for i in range(0, svm.size, step)]


def analyze_motion(imu: Optional[Sequence],
                   imu_ts_ms: Optional[Sequence] = None,
                   imu_sr: int = IMU_SAMPLE_RATE) -> Dict[str, Any]:
    """청크 구간 IMU → 1초 창별 stable/motion/no_imu 라벨 → 청크 단위 motion 여부."""
    if imu is None:
        return {"checked": False, "reason": "no_imu_input"}
    svm = _to_svm(imu)
    if svm.size == 0:
        return {"checked": False, "reason": "empty_imu"}

    labels = [_motion_label(w) for w in _split_windows(svm, imu_ts_ms, imu_sr)]
    n_motion = labels.count("motion")
    n_stable = labels.count("stable")
    n_labeled = n_motion + n_stable

    if n_labeled < MOTION_MIN_WINDOWS:
        return {"checked": False, "reason": "too_few_imu_windows",
                "n_motion": n_motion, "n_stable": n_stable, "n_no_imu": labels.count("no_imu")}

    ratio = n_motion / n_labeled
    return {"checked": True, "is_motion": ratio >= MOTION_RATIO_THR, "motion_ratio": ratio,
            "n_motion": n_motion, "n_stable": n_stable, "n_no_imu": labels.count("no_imu")}


# -------------------- PPG --------------------
def analyze_ppg_chunk(ppg_values: List[float],
                      imu: Optional[Sequence] = None,
                      imu_ts_ms: Optional[Sequence] = None,
                      imu_sr: int = IMU_SAMPLE_RATE) -> Dict[str, Any]:
    access_log.error("[WEAR_PROBE] analyze_ppg_chunk ENTER")

    try:
        # 전처리
        data_raw = _preprocess_green(ppg_values, SAMPLE_RATE)
        if data_raw.size == 0:
            access_log.info("[WEAR_DBG] empty_or_all_nonnumeric")
            return {"result": "invalid", "reason": "empty_or_all_nonnumeric"}

        # Motion 판정 (IMU 미전달 시 checked=False → 기존 동작 그대로)
        motion = analyze_motion(imu, imu_ts_ms, imu_sr)
        access_log.info(f"[WEAR_DBG] motion={motion}")

        # 필터링
        filtered = hp.filter_signal(
            data_raw, [0.5, 8], sample_rate=SAMPLE_RATE, order=3, filtertype='bandpass'
        )

        # amp/std 판정은 THRESHOLD_LOW/HIGH가 캘리브레이션된 스케일과 맞도록
        # 대역통과 필터 이전(data_raw) 신호 기준으로 계산한다.
        amp = float(np.ptp(data_raw))
        std = float(np.std(data_raw))
        access_log.info(f"[WEAR_DBG] len={len(filtered)} amp={amp:.2f} std={std:.2f}")

        # HeartPy 처리
        try:
            wd, m = hp.process(filtered, sample_rate=SAMPLE_RATE)
            bpm = float(m.get("bpm", 0.0))
            peaks = len(wd.get("peaklist", []))
            removed = len(wd.get("removed_beats", []))
            access_log.info(f"[WEAR_DBG] bpm={bpm:.1f} peaks={peaks} removed={removed}")
        except Exception as e:
            access_log.info(f"[WEAR_DBG] HeartPy 처리 실패: {type(e).__name__} - {e}")
            return {"result": "error", "detail": str(e)}

        # 유효성 판단 (생리적 기준)
        is_valid = (
            (30 < bpm < 200)
            and (peaks - removed) > (len(filtered) / SAMPLE_RATE) / 2
        )

        if not is_valid:
            access_log.info(f"[WEAR_DBG] bpm={bpm:.1f} peaks={peaks} removed={removed}")
            if amp < THRESHOLD_LOW or amp > THRESHOLD_HIGH:
                access_log.info(f"[WEAR_DBG] → fallback: non_wear (amp={amp:.1f})")
                return {"result": "non_wear", "reason": "fallback_amp_check"}
            else:
                return {"result": "invalid", "reason": "physio", "motion": motion}

        # 유효성 판단 (motion 기준): 생리적으로는 통과했어도 움직임 구간이면 invalid
        if motion.get("checked") and motion.get("is_motion"):
            access_log.info(f"[WEAR_DBG] → invalid: motion (ratio={motion['motion_ratio']:.2f})")
            return {"result": "invalid", "reason": "motion", "motion": motion}

        chunk_len = len(data_raw)
        subchunk_len = chunk_len // SUBCHUNK_COUNT
        votes = []

        for i in range(SUBCHUNK_COUNT):
            sub = data_raw[i * subchunk_len : (i + 1) * subchunk_len]
            amp_sub = np.ptp(sub)
            std_sub = np.std(sub)
            state = "wear" if (THRESHOLD_LOW <= amp_sub <= THRESHOLD_HIGH) else "non_wear"
            votes.append(state)
            access_log.info(f"[WEAR_DBG] sub{i+1}: amp={amp_sub:.1f} std={std_sub:.1f} state={state}")

        wear_cnt = votes.count("wear")
        non_cnt = votes.count("non_wear")
        final = "wear" if wear_cnt >= non_cnt else "non_wear"

        access_log.info(f"[WEAR_DBG] final={final} ({wear_cnt}:{non_cnt})")

        return {"result": final}

    except Exception as e:
        access_log.exception(f"[WEAR_DBG] exception: {type(e).__name__}: {e}")
        return {"result": "error", "detail": str(e)}


def wear_green_to_pred(ppg_green: List[float],
                       imu: Optional[Sequence] = None,
                       imu_ts_ms: Optional[Sequence] = None,
                       imu_sr: int = IMU_SAMPLE_RATE) -> Dict[str, Any]:
    """Analyze → ModelPredSerializer 형태로 매핑. imu 미전달 시 기존과 동일하게 동작."""
    print(f"[WEAR_PROBE] >>> wear_green_to_pred CALLED len={len(ppg_green) if ppg_green else 0}")
    try:
        r = analyze_ppg_chunk(ppg_green or [], imu=imu, imu_ts_ms=imu_ts_ms, imu_sr=imu_sr)
        res = r.get("result", "error")

        if res == "wear":
            # 착용: label=1, valid=True
            return {"prob": None, "label": 1, "thr": None, "valid": True, "error": None}
        if res == "non_wear":
            # 비착용: label=0, valid=True
            return {"prob": None, "label": 0, "thr": None, "valid": True, "error": None}
        if res == "invalid":
            # 신호는 들어왔지만 판정 불가 (reason: physio / motion / empty_or_all_nonnumeric 은 로그로 확인)
            return {"prob": None, "label": None, "thr": None, "valid": False, "error": "invalid"}
        # error 포함
        return {"prob": None, "label": None, "thr": None, "valid": False, "error": r.get("detail") or "error"}
    except Exception as e:
        return {"prob": None, "label": None, "thr": None, "valid": False, "error": f"wear_detect_failed: {e}"}
