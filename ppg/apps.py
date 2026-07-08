import logging
from django.apps import AppConfig
from django.db.models.signals import post_save

logger = logging.getLogger(__name__)


class PpgConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "ppg"

    def ready(self):
        try:
            self._load_model()
            self._restore_baselines()
            self._connect_signals()
        except Exception as e:
            logger.error(f"[PPGConfig] ready() failed: {e}")

    def _load_model(self):
        from pathlib import Path
        from django.conf import settings

        base_dir   = Path(getattr(settings, "BASE_DIR", Path(__file__).resolve().parents[1]))
        model_path = base_dir / "media" / "models" / "ppg" / "best_c_stream_change_model.pt"

        if not model_path.exists():
            logger.warning(f"[PPGConfig] model not found: {model_path}")
            return

        from .apnea_engine import ApneaEngine
        ok = ApneaEngine.get_instance().load_model(str(model_path))
        if ok:
            logger.info("[PPGConfig] model ready")
        else:
            logger.error("[PPGConfig] model load failed")

    def _restore_baselines(self):
        try:
            import numpy as np
            import torch
            from .apnea_engine import (ApneaEngine, RealtimeApneaDetector,
                                       RealtimeBeatExtractor, FS)
            from .models import ApneaSession

            from django.db import connection
            if 'ppg_apneasession' not in connection.introspection.table_names():
                logger.warning("[restore] ppg_apneasession not ready, skip")
                return

            engine = ApneaEngine.get_instance()
            if not engine.model_ready:
                logger.warning("[restore] model not ready, skip")
                return

            seen     = set()
            sessions = ApneaSession.objects.filter(baseline_ready=True).order_by('-id')

            for session in sessions:
                if session.device_id in seen:
                    continue
                seen.add(session.device_id)

                try:
                    device_id   = session.device_id
                    stats       = session.baseline_stats

                    if not stats:
                        continue

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
                        fs=FS, rolling_seconds=13.0, safe_margin_seconds=0.6
                    )

                    with engine._dev_lock:
                        engine._detectors[device_id]     = detector
                        engine._extractors[device_id]    = extractor
                        engine._baseline_done[device_id] = True
                        engine._baseline_buf[device_id]  = []
                        engine._packet_count[device_id]  = 999

                    logger.info(f"[restore] baseline restored: {device_id}")

                except Exception as e:
                    logger.warning(f"[restore] failed for {session.device_id}: {e}")

        except Exception as e:
            logger.warning(f"[restore] baseline restore failed: {e}")

    def _connect_signals(self):
        try:
            from monitoring.models import SensorWindow
            from .signals import handle_sensor_window
            post_save.connect(handle_sensor_window, sender=SensorWindow)
            logger.info("[PPGConfig] SensorWindow signal connected")
        except Exception as e:
            logger.error(f"[PPGConfig] signal connect failed: {e}")