import logging
from django.db.models.signals import post_save
from django.dispatch import receiver
import datetime


logger = logging.getLogger(__name__)


def handle_sensor_window(sender, instance, created, **kwargs):
    if not created:
        return

    try:
        from .apnea_engine import ApneaEngine
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
            apnea_session = _get_or_create_apnea_session(device_id, sw.started_at)
            if not engine._baseline_active.get(device_id, False):
                engine.start_session(device_id, session_pk=apnea_session.pk)
            result = engine.process_chunk(
                device_id, ppg_green, ppg_ir=[], ppg_red=[],
                session_db=apnea_session, packet_timestamp=sw.started_at,
            )

        elif mode == 'THREAT':
            apnea_session = _get_latest_apnea_session(device_id)
           # ★ worker가 baseline 상태를 모르면 DB에서 즉시 복원
            if apnea_session and not engine._baseline_done.get(device_id, False):
                _restore_baseline_if_needed(engine, device_id, apnea_session)

            result = engine.process_chunk(
                device_id, ppg_green, ppg_ir=[], ppg_red=[],
                session_db=apnea_session, packet_timestamp=sw.started_at,
            )

        else:
            return

        # ── abs_ts 계산 ────────────────────────────────
        # t0 = 엔진이 기억하는 첫 번째 패킷 시각
        # time_sec = sp_sample / 25 (서버 시작 이후 누적 초)
        # abs_ts = t0 + time_sec
        beat_results = result.get("beat_results") or []
        t0 = engine._t0.get(device_id)

        if beat_results and t0:
            for beat in beat_results:
                time_sec = beat.get("time_sec", 0)
                abs_dt   = t0 + datetime.timedelta(seconds=time_sec)
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

    except Exception as e:
        logger.error(f"[signal] handle_sensor_window failed: {e}")


def _restore_baseline_if_needed(engine, device_id, apnea_session):
    """worker가 baseline 상태를 모를 때 DB에서 복원"""
    try:
        import numpy as np
        import torch
        from .apnea_engine import (RealtimeApneaDetector,
                                   RealtimeBeatExtractor, FS,
                                   normalize_feature_vector)

        stats = apnea_session.baseline_stats
        if not stats:
            logger.warning(f"[restore] {device_id}: baseline_stats 없음")
            return

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

        extractor = RealtimeBeatExtractor(
            fs=FS, rolling_seconds=20.0, safe_margin_seconds=1.0,
            min_rr_sec=0.45, max_rr_sec=1.5,
        )

        with engine._dev_lock:
            engine._detectors[device_id]      = detector
            engine._extractors[device_id]     = extractor
            engine._baseline_done[device_id]  = True
            engine._baseline_buf[device_id]   = []
            engine._packet_count[device_id]   = 999
            engine._baseline_active[device_id] = False

        logger.info(f"[restore] on-demand baseline restored: {device_id}")

    except Exception as e:
        logger.error(f"[restore] failed: {e}")


def _get_or_create_apnea_session(device_id, started_at):
    from .models import ApneaSession
    session = (ApneaSession.objects
               .filter(device_id=device_id, baseline_ready=False)
               .order_by('-started_at')
               .first())
    if session:
        return session
    return ApneaSession.objects.create(
        device_id=device_id,
        started_at=started_at,
        baseline_ready=False,
    )


def _get_latest_apnea_session(device_id):
    from .models import ApneaSession
    return (ApneaSession.objects
            .filter(device_id=device_id, baseline_ready=True)
            .order_by('-started_at')
            .first())