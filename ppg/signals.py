import logging
from django.db.models.signals import post_save
from django.dispatch import receiver
import datetime


logger = logging.getLogger(__name__)


CHUNK_DURATION_SEC = 12
CHUNK_STEP_SEC = 6

# 유효 beat가 적으면 비율이 극단으로 튄다(2개 중 2개 = 100% -> 레벨5).
# 측정 재개 경계나 신호 불량 구간이 위험으로 오탐되는 것을 막는다.
MIN_CHUNK_BEATS = 8

# 청크 위협 확률 구간 -> (threat_detected_log, threat_detected) 고정 매핑
PPG_PROBABILITY_THRESHOLDS = (
    (0.2, 1, False),
    (0.4, 2, False),
    (0.6, 3, False),
    (0.8, 4, True),
)


def _ppg_probability_to_risk(probability):
    for upper, threat_detected_log, threat_detected in PPG_PROBABILITY_THRESHOLDS:
        if probability < upper:
            return threat_detected_log, threat_detected
    return 5, True


def _save_ppg_analysis_result(device_id, mode, timestamp, probability):
    if probability is None:
        return None

    from analysis.models import Result

    threat_detected_log, threat_detected = _ppg_probability_to_risk(probability)

    return Result.objects.create(
        device_id=device_id,
        mode=mode,
        event_type=Result.EventType.PPG,
        timestamp=timestamp,
        probability=probability,
        threat_detected_log=threat_detected_log,
        threat_detected=threat_detected,
    )


def _collect_chunk_labels(device_id, session_id, start, end):
    """[start, end) 구간에 abs_ts가 속하는 beat들의 pred_label 목록.

    직전 세션이 끝나자마자 새 세션이 시작되면 시간 범위만으로는 이전 세션 beat가
    섞이므로, 같은 모니터링 세션의 윈도우만 본다.
    """
    from django.utils.dateparse import parse_datetime

    from .models import ApneaResult

    # rolling buffer/safe margin 때문에 beat가 자신보다 늦게 시작한 윈도우 row에
    # 기록될 수 있어, 앞뒤로 한 윈도우씩 여유를 두고 조회한다.
    margin = datetime.timedelta(seconds=CHUNK_DURATION_SEC)

    rows = (ApneaResult.objects
            .filter(
                device_id=device_id,
                sensor_window__session_id=session_id,
                sensor_window__started_at__gte=start - margin,
                sensor_window__started_at__lt=end + margin,
            )
            .values_list('beat_results', flat=True))

    labels = []
    for beats in rows:
        for beat in beats or []:
            label = beat.get('pred_label')
            abs_ts = beat.get('abs_ts')

            if label is None or not abs_ts:
                continue

            ts = parse_datetime(abs_ts)
            if ts is not None and start <= ts < end:
                labels.append(int(label))

    return labels


def _chunk_wear_valid(device_id, session_id, start, end):
    from django.db.models import Q
    from .models import ApneaResult

    window_len = datetime.timedelta(seconds=CHUNK_DURATION_SEC)

    rows = ApneaResult.objects.filter(
        Q(sensor_window__ended_at__gt=start)
        | Q(sensor_window__ended_at__isnull=True,
            sensor_window__started_at__gt=start - window_len),
        device_id=device_id,
        sensor_window__session_id=session_id,
        sensor_window__started_at__lt=end,
    )

    return rows.exists() and not rows.exclude(wear_valid=True).exists()


def _save_ppg_chunk_results(device_id, mode, sensor_window):
    """구간이 모두 채워진 청크(12초 길이, 6초 슬라이딩)를 계산해 저장.

    이번 윈도우의 beat까지 기록된 시점이므로, 이번 윈도우 시작 시각보다
    12초/6초 앞선 두 청크가 마감된다. 시간 순서대로 저장한다.
    세션 시작보다 앞서는 청크는 이 세션의 결과가 아니므로 저장하지 않는다.
    """
    saved = []
    session_started_at = _session_started_at(sensor_window)

    for offset_sec in (CHUNK_DURATION_SEC, CHUNK_STEP_SEC):
        start = sensor_window.started_at - datetime.timedelta(seconds=offset_sec)
        end = start + datetime.timedelta(seconds=CHUNK_DURATION_SEC)

        if start < session_started_at:
            continue

        if not _chunk_wear_valid(
            device_id, sensor_window.session_id, start, end
        ):
            logger.debug(
                f"[ppg-chunk] {device_id} {start.isoformat()} "
                f"skip: wear invalid"
            )
            continue

        labels = _collect_chunk_labels(
            device_id, sensor_window.session_id, start, end
        )
        if len(labels) < MIN_CHUNK_BEATS:
            continue

        probability = sum(labels) / len(labels)

        _save_ppg_analysis_result(
            device_id=device_id,
            mode=mode,
            timestamp=start,
            probability=probability,
        )
        saved.append((start, probability, len(labels)))

    return saved


def handle_sensor_window(sender, instance, created, **kwargs):
    if not created:
        return

    try:
        from .apnea_engine import ApneaEngine, MIN_BASELINE_SAMPLES
        from .models import ApneaSession, ApneaResult

        sw        = instance
        session   = sw.session
        mode      = session.mode
        device_id = session.protectee.device_id

        ppg_green = sw.ppg_green
        if not isinstance(ppg_green, list) or len(ppg_green) == 0:
            return

        engine = ApneaEngine.get_instance()

        if mode == 'CALIBRATION':
            apnea_session = _get_or_create_apnea_session(
                device_id, _session_started_at(sw)
            )
            # 수집 중인 대상이 바뀌면(다른 캘리브레이션 회차) 버퍼를 비우고 다시 시작해야
            # 이전 회차 샘플이 섞이지 않는다.
            if (not engine._baseline_active.get(device_id, False)
                    or engine._session_pk.get(device_id) != apnea_session.pk):
                engine.start_session(device_id, session_pk=apnea_session.pk)
                # ★ t0를 apnea_session.started_at으로 명시 설정
                engine._t0[device_id] = apnea_session.started_at
            result = engine.process_chunk(

                device_id, ppg_green, ppg_ir=[], ppg_red=[],

                session_db=apnea_session, packet_timestamp=sw.started_at,

            )
               # baseline이 아직 완료 안 됐는데 충분한 샘플이 DB에 있으면 강제 완료
            if not engine._baseline_done.get(device_id, False):
                from monitoring.models import SensorWindow as SW
                all_ppg = []
                for w in SW.objects.filter(
                    session__protectee__device_id=device_id,
                    session__mode='CALIBRATION',
                    started_at__gte=apnea_session.started_at,  # ← 현재 세션 시작 이후만

                ).order_by('started_at'):
                    all_ppg.extend(w.ppg_green)

                if len(all_ppg) >= MIN_BASELINE_SAMPLES:
                    logger.info(f"[CAL] 강제 baseline 수집: {device_id} 샘플={len(all_ppg)}")
                    engine._baseline_buf[device_id] = all_ppg  # 기존 버퍼 교체
                    engine._finalize_baseline(device_id, apnea_session)
        elif mode in ('THREAT', 'PERIODIC'):
            apnea_session = _get_latest_apnea_session(device_id)

            if not apnea_session:
                return

            current_baseline_pk = engine._baseline_session_pk.get(device_id)
            current_monitoring_pk = engine._monitoring_session_pk.get(device_id)

            # 모니터링 세션이 바뀌면 이전 세션의 추론 상태(hysteresis, smoother,
            # beat_window, raw_buffer)를 버리고 베이스라인 기준으로 다시 시작한다.
            # 그대로 두면 위험 상태로 끝난 세션 직후 새 세션도 위험으로 시작한다.
            if (current_baseline_pk != apnea_session.pk
                    or current_monitoring_pk != session.pk):
                # 복원에 실패했는데 세션 pk를 갱신하면 이전 세션 상태로 추론이 이어지고,
                # 이후 윈도우에서도 다시 복원하지 않는다. 이번 윈도우는 건너뛰고 다음
                # 윈도우에서 다시 시도한다.
                if not _restore_baseline_if_needed(engine, device_id, apnea_session):
                    logger.warning(
                        f"[signal] {device_id}: baseline 복원 실패 -> 윈도우 건너뜀 "
                        f"(session={session.pk}, sw={sw.pk})"
                    )
                    return
                engine._baseline_session_pk[device_id] = apnea_session.pk
                engine._monitoring_session_pk[device_id] = session.pk
                engine._t0[device_id] = sw.started_at

            imu = None
            if sw.x and sw.y and sw.z:
                import numpy as np
                imu = np.stack([sw.x, sw.y, sw.z], axis=1)  # (N, 3)

            result = engine.process_chunk(
                device_id, ppg_green,
                ppg_ir=[],
                ppg_red=[],
                session_db=apnea_session,
                packet_timestamp=sw.started_at,
                imu=imu,
            )
        else:
            return

        # # ── abs_ts 계산 ────────────────────────────────
        # beat_results = result.get("beat_results") or []
        # t0 = engine._t0.get(device_id)

        # # t0가 없으면 apnea_session.started_at으로 설정
        # if not t0 and apnea_session:
        #     t0 = apnea_session.started_at
        #     engine._t0[device_id] = t0

        # if beat_results and t0:
        #     for beat in beat_results:
        #         time_sec = beat.get("time_sec", 0)
        #         abs_dt   = t0 + datetime.timedelta(seconds=time_sec)
        #         beat["abs_ts"] = abs_dt.isoformat()

        # ── abs_ts 계산 ────────────────────────────────
        from .apnea_engine import FS

        beat_results = result.get("beat_results") or []
        extractor = engine._extractors.get(device_id)

        if beat_results and extractor is not None:
            packet_n = len(ppg_green)

            # 현재 SensorWindow의 첫 샘플이 extractor 전체에서 몇 번째인지
            packet_start_global = (
                extractor.global_sample_idx - packet_n + 1
            )

            for beat in beat_results:
                sp_sample = beat.get("sp_sample")
                if sp_sample is None:
                    continue

                # 현재 SensorWindow 시작 기준 상대 sample 위치
                # 음수도 허용: rolling buffer 때문에 직전 window의 beat일 수 있음
                offset_sample = sp_sample - packet_start_global

                abs_dt = (
                    sw.started_at
                    + datetime.timedelta(seconds=offset_sample / FS)
                )

                beat["abs_ts"] = abs_dt.isoformat()

        wear = result.get("wear", {})
        ApneaResult.objects.update_or_create(
            sensor_window=sw,
            defaults={
                'device_id':      device_id,
                'wear_valid':     wear.get("valid"),
                'wear_label':     wear.get("label"),
                'is_baseline':    (result["phase"] == "baseline"),
                'beat_results':   result.get("beat_results") or None,
                'p_apnea':        result.get("p_apnea"),
                'p_apnea_smooth': result.get("p_apnea_smooth"),
                'pred_label':     result.get("pred_label"),
                'pred_status':    result.get("pred_status"),
            }
        )
        logger.debug(f"[signal] {device_id} mode={mode} phase={result['phase']}")

        if mode in ('THREAT', 'PERIODIC') and result["phase"] == "inference":
            saved = _save_ppg_chunk_results(device_id, mode, sw)
            for start, probability, beat_count in saved:
                logger.debug(
                    f"[ppg-chunk] {device_id} {start.isoformat()} "
                    f"p={probability:.3f} beats={beat_count}"
                )

    except Exception as e:
        logger.error(f"[signal] handle_sensor_window failed: {e}")


def _restore_baseline_if_needed(engine, device_id, apnea_session):
    """worker가 baseline 상태를 모를 때 DB에서 복원. 성공 여부를 반환한다."""
    try:
        import numpy as np
        import torch
        from .apnea_engine import RealtimeApneaDetector, make_beat_extractor

        stats = apnea_session.baseline_stats
        if not stats:
            logger.warning(f"[restore] {device_id}: baseline_stats 없음")
            return False

        ref_mu      = np.array(stats['ref_mu'], dtype=np.float32)
        ref_sd      = np.array(stats['ref_sd'], dtype=np.float32)
        cfg         = engine._model_cfg or {}
        context_len = int(cfg.get('context_len', 20))
        threshold   = float(cfg.get('threshold', 0.75))
        device_str  = 'cuda' if torch.cuda.is_available() else 'cpu'

        detector = RealtimeApneaDetector(
            model                = engine._model,
            context_len          = context_len,
            ref_mu               = ref_mu,
            ref_sd               = ref_sd,
            threshold_from_train = threshold,
            device               = device_str,
        )

        last_beats = stats.get('last_beats') or []
        for vec in last_beats:
            detector.beat_window.append(np.array(vec, dtype=np.float32))

        extractor = make_beat_extractor()

        with engine._dev_lock:
            engine._detectors[device_id]      = detector
            engine._extractors[device_id]     = extractor
            engine._baseline_done[device_id]  = True
            engine._baseline_buf[device_id]   = []
            engine._packet_count[device_id]   = 999
            engine._baseline_active[device_id] = False

        logger.info(f"[restore] on-demand baseline restored: {device_id}")
        return True

    except Exception as e:
        logger.error(f"[restore] failed: {e}")
        return False


def _session_started_at(sensor_window):
    """윈도우가 속한 세션의 첫 윈도우 시각(워치 시계 기준).

    서버의 세션 시작 시각은 워치 시계와 어긋나므로 첫 윈도우 시각을 기준으로 쓴다.
    캘리브레이션에서는 ApneaSession.started_at이 회차 도중 윈도우 시각으로 잡히면
    그보다 앞선 윈도우가 베이스라인 집계에서 통째로 빠지므로, 항상 첫 윈도우로 맞춘다.
    """
    from monitoring.models import SensorWindow

    first = (SensorWindow.objects
             .filter(session_id=sensor_window.session_id)
             .order_by('started_at')
             .values_list('started_at', flat=True)
             .first())

    return first or sensor_window.started_at


def _get_or_create_apnea_session(device_id, started_at):
    from .models import ApneaSession

    # 같은 회차(= 같은 시작 시각)만 재사용한다. 이전 회차의 미완료 세션을 물려받으면
    # started_at이 어긋나 집계 범위가 틀어진다.
    session = (ApneaSession.objects
               .filter(device_id=device_id, baseline_ready=False, started_at=started_at)
               .first())
    if session:
        return session
    return ApneaSession.objects.create(
        device_id=device_id,
        started_at=started_at,
        baseline_ready=False,
    )


def _get_latest_apnea_session(device_id):
    """
    is_selected=True인 세션을 우선 반환.
    선택된 세션이 없으면 가장 최근 baseline_ready 세션을 반환.
    """
    from .models import ApneaSession
    selected = (ApneaSession.objects
                .filter(device_id=device_id, baseline_ready=True, is_selected=True)
                .order_by('-started_at')
                .first())
    if selected:
        return selected
    return (ApneaSession.objects
            .filter(device_id=device_id, baseline_ready=True)
            .order_by('-started_at')
            .first())