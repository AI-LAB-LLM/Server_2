import json
import logging
from datetime import datetime, timezone

from django.shortcuts import render
from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from .apnea_engine import ApneaEngine
from .models import ApneaSession, ApneaResult

logger = logging.getLogger(__name__)


def _get_device_id(sensor_window) -> str:
    return sensor_window.session.protectee.device_id



# ─────────────────────────────────────────────
# Apnea (SensorWindow 기반)


class ApneaRecordsView(View):
    """
    GET /ppg/api/apnea/records/?device_id=...&limit=120
    """
    def get(self, request):
        device_id = request.GET.get("device_id")
        try:
            limit = int(request.GET.get("limit", 120))
        except ValueError:
            limit = 120

        qs = ApneaResult.objects.select_related('sensor_window').order_by("-processed_at")
        if device_id:
            qs = qs.filter(device_id=device_id) 
        results = list(qs[:limit])
        results.reverse()

        items = []
        for r in results:
            sw = r.sensor_window
            item = {
                "id":               r.pk,
                "sensor_window_id": sw.pk,
                "session_id":       sw.session_id,   # 추가
                "device_id":        r.device_id,
                "timestamp":        sw.started_at.isoformat(),
                "ended_at":         sw.ended_at.isoformat() if sw.ended_at else None,
                "created_at":       sw.created_at.isoformat(),
                "is_baseline":      r.is_baseline,
                "beat_results":     r.beat_results or [],
                "predictions": {
                    "WEAR_GREEN": {
                        "valid": r.wear_valid,
                        "label": r.wear_label,
                    },
                },
            }
            if not r.is_baseline and r.p_apnea_smooth is not None:
                item["predictions"]["APNEA_RESULT"] = {
                    "prob":   r.p_apnea_smooth,
                    "label":  r.pred_label,
                    "valid":  r.pred_status == "ok",
                    "status": r.pred_status,
                }
            items.append(item)

        return JsonResponse({"ok": True, "items": items, "total": len(items)})


class ModelStatusView(View):
    """GET /ppg/api/apnea/status/"""
    def get(self, request):
        engine = ApneaEngine.get_instance()
        return JsonResponse({
            "model_ready":  engine.model_ready,
            "model_config": engine.model_config,
        })


# ─────────────────────────────────────────────
# Apnea 대시보드 (device별)
# ─────────────────────────────────────────────

class DeviceDashboardView(View):
    """GET /ppg/device/<device_id>/"""
    def get(self, request, device_id):
        from django.conf import settings

        try:
            from monitoring.models import Protectee
            protectee = Protectee.objects.get(device_id=device_id)
            protectee_name = protectee.name or device_id
        except Exception:
            protectee_name = device_id

        results = (ApneaResult.objects
                   .filter(device_id=device_id)
                   .select_related('sensor_window')
                   .order_by('-processed_at')[:120])
        results = list(reversed(results))

        items = []
        for r in results:
            sw = r.sensor_window
            item = {
                "id":               r.pk,
                "sensor_window_id": sw.pk,
                "session_id":       sw.session_id,   # 추가
                "device_id":        r.device_id,
                "timestamp":        sw.started_at.isoformat(),
                "ended_at":         sw.ended_at.isoformat() if sw.ended_at else None,
                "created_at":       sw.created_at.isoformat(),
                "is_baseline":      r.is_baseline,
                "beat_results":     r.beat_results or [],
                "predictions": {
                    "WEAR_GREEN": {
                        "valid": r.wear_valid,
                        "label": r.wear_label,
                    },
                },
            }
            if not r.is_baseline and r.p_apnea_smooth is not None:
                item["predictions"]["APNEA_RESULT"] = {
                    "prob":   r.p_apnea_smooth,
                    "label":  r.pred_label,
                    "valid":  r.pred_status == "ok",
                    "status": r.pred_status,
                }
            items.append(item)

        kakao_key = getattr(settings, "KAKAO_JS_KEY", "")

        return render(request, "ppg/dashboard_device.html", {
            "device_id":      device_id,
            "protectee_name": protectee_name,
            "items":          items,
            "KAKAO_JS_KEY":   kakao_key,
        })


IMU_HISTORY_SEC = 180

class BaselineListView(View):
    """
    GET  /ppg/api/baseline/?device_id=...
    해당 device의 베이스라인 목록 반환 (최대 20개)
    """
    FEAT_LABELS = ["FO_SP_s", "Downstroke_vel", "HR_bpm", "RR_s", "QI"]

    def get(self, request):
        device_id = request.GET.get("device_id")
        if not device_id:
            return JsonResponse({"ok": False, "error": "device_id required"}, status=400)

        sessions = (ApneaSession.objects
                    .filter(device_id=device_id, baseline_ready=True)
                    .order_by('-started_at')[:20])

        items = []
        for s in sessions:
            stats = s.baseline_stats or {}
            num_beats = stats.get("num_beats", 0)

            ref_mu = stats.get("ref_mu", [])
            ref_sd = stats.get("ref_sd", [])
            feat_stats = []
            for i, label in enumerate(self.FEAT_LABELS):
                feat_stats.append({
                    "feature": label,
                    "mu":  round(ref_mu[i], 4) if i < len(ref_mu) else None,
                    "std": round(ref_sd[i], 4) if i < len(ref_sd) else None,
                })

            items.append({
                "id":          s.pk,
                "started_at":  s.started_at.isoformat(),
                "num_beats":   num_beats,
                "is_selected": s.is_selected,
                "feat_stats":  feat_stats,
            })

        return JsonResponse({"ok": True, "items": items})


@method_decorator(csrf_exempt, name='dispatch')
class BaselineSelectView(View):
    """
    POST /ppg/api/baseline/select/
    body: {"device_id": "...", "session_id": <pk>}
    선택된 baseline을 is_selected=True로 표시, 나머지는 False
    """
    def post(self, request):
        try:
            body = json.loads(request.body)
            device_id  = body.get("device_id")
            session_id = body.get("session_id")
            if not device_id or not session_id:
                return JsonResponse({"ok": False, "error": "device_id, session_id required"}, status=400)

            ApneaSession.objects.filter(device_id=device_id).update(is_selected=False)
            updated = ApneaSession.objects.filter(pk=session_id, device_id=device_id).update(is_selected=True)
            if updated == 0:
                return JsonResponse({"ok": False, "error": "session not found"}, status=404)

            return JsonResponse({"ok": True, "selected": session_id})
        except Exception as e:
            return JsonResponse({"ok": False, "error": str(e)}, status=500)

class EventStatusView(View):
    """
    GET /ppg/api/event_status/?device_id=...
    최신 IMU 레벨 + 최신 결과 기준 최근 3분 레벨 이력(history)
    """
    def get(self, request):
        from datetime import timedelta
        from analysis.models import Result
        from django.utils import timezone

        device_id = request.GET.get("device_id")
        if not device_id:
            return JsonResponse({"ok": False, "error": "device_id required"}, status=400)

        imu_qs = Result.objects.filter(device_id=device_id, event_type=Result.EventType.IMU)

        # 3분 이내 데이터만
        three_min_ago = timezone.now() - timedelta(seconds=IMU_HISTORY_SEC)

        latest = (imu_qs
                  .filter(timestamp__gte=three_min_ago)
                  .order_by("-timestamp", "-id")
                  .first())

        if not latest:
            return JsonResponse({
                "ok":               False,
                "imu_display":      "데이터 없음",
                "imu_danger_level": None,
                "timestamp":        None,
                "history":          [],
            })

        # 윈도우 업로드 지연으로 최신 결과가 now보다 늦으므로, 최신 결과 시각을 기준으로 3분을 자른다.
        # offset_sec: 최신 결과로부터 몇 초 전인지 (막대 슬롯 배치용, 클라이언트 시계와 무관)
        rows = (imu_qs
                .filter(timestamp__gt=latest.timestamp - timedelta(seconds=IMU_HISTORY_SEC),
                        timestamp__lte=latest.timestamp)
                .order_by("timestamp", "id")
                .values("id", "timestamp", "threat_detected_log"))
        history = [
            {
                "id":         row["id"],
                "timestamp":  row["timestamp"],
                "level":      row["threat_detected_log"],
                "offset_sec": (latest.timestamp - row["timestamp"]).total_seconds(),
            }
            for row in rows
        ]

        level = latest.threat_detected_log
        if level is None:
            display = "데이터 없음"
        elif level >= 4:
            display = "위험"
        elif level >= 2:
            display = "주의"
        else:
            display = "안정"

        return JsonResponse({
            "ok":               True,
            "imu_display":      display,
            "imu_danger_level": level,
            "timestamp":        latest.timestamp,
            "result_id":        latest.id,
            "history":          history,
        })

# ─────────────────────────────────────────────
# SOS (대시보드 상단 배너)
# ─────────────────────────────────────────────

def _active_sos(device_id):
    """종결되지 않은 가장 최근 SOS (세션, SOS Result). 없으면 (None, None).

    SOS Result가 저장되기 전(플랫폼 전송 중)에는 표시하지 않는다.
    """
    from analysis.models import Result
    from monitoring.models import MonitoringSession

    session = (MonitoringSession.objects
               .filter(protectee__device_id=device_id,
                       sos_detected_at__isnull=False,
                       sos_closed_at__isnull=True)
               .order_by("-sos_detected_at")
               .first())
    if session is None:
        return None, None

    sos = (Result.objects
           .filter(device_id=device_id,
                   event_type=Result.EventType.SOS,
                   created_at__gte=session.sos_detected_at)
           .order_by("created_at")
           .first())
    return session, sos


def _sos_basis(device_id, t):
    """SOS를 일으킨 시점 t 기준 판정 근거 재계산 (판정 로직과 동일한 evaluate_sos 사용)."""
    from datetime import timedelta
    from django.db.models import Max
    from analysis.models import Result
    from analysis.sos_services import SOS_WINDOW_SEC, evaluate_sos

    _, detail = evaluate_sos(device_id, t)

    window = Result.objects.filter(
        device_id=device_id,
        timestamp__gt=t - timedelta(seconds=SOS_WINDOW_SEC),
        timestamp__lte=t,
    )
    for event_type, key in ((Result.EventType.IMU, "imu_max_level"),
                            (Result.EventType.PPG, "ppg_max_level")):
        detail[key] = (window.filter(event_type=event_type)
                       .aggregate(m=Max("threat_detected_log"))["m"])
    return detail


class SosStatusView(View):
    """GET /ppg/api/sos/status/?device_id=..."""
    def get(self, request):
        from django.utils import timezone

        device_id = request.GET.get("device_id")
        if not device_id:
            return JsonResponse({"ok": False, "error": "device_id required"}, status=400)

        session, sos = _active_sos(device_id)
        if sos is None:
            return JsonResponse({"ok": True, "active": False})

        try:
            basis = _sos_basis(device_id, sos.timestamp)
        except Exception as e:
            logger.error(f"[sos] 판정 근거 계산 실패 (result_id={sos.id}): {e}")
            basis = {"rule": None}

        return JsonResponse({
            "ok":     True,
            "active": True,
            "sos": {
                "session_id":  session.id,
                "result_id":   sos.id,
                "occurred_at": sos.created_at,
                "elapsed_sec": max(0.0, (timezone.now() - sos.created_at).total_seconds()),
                **basis,
            },
        })


@method_decorator(csrf_exempt, name="dispatch")
class SosCloseView(View):
    """POST /ppg/api/sos/close/  {"device_id": ..., "session_id": ...}

    해당 세션까지의 미종결 SOS를 모두 종결한다. 이후 새 세션에서 SOS가 나면 다시 표시된다.
    """
    def post(self, request):
        from django.utils import timezone
        from monitoring.models import MonitoringSession

        try:
            body = json.loads(request.body or b"{}")
        except ValueError:
            return JsonResponse({"ok": False, "error": "invalid json"}, status=400)

        device_id  = body.get("device_id")
        session_id = body.get("session_id")
        if not device_id or not isinstance(session_id, int):
            return JsonResponse({"ok": False, "error": "device_id, session_id required"}, status=400)

        target = (MonitoringSession.objects
                  .filter(pk=session_id, protectee__device_id=device_id,
                          sos_detected_at__isnull=False)
                  .first())
        if target is None:
            return JsonResponse({"ok": False, "error": "sos session not found"}, status=404)

        closed = (MonitoringSession.objects
                  .filter(protectee__device_id=device_id,
                          sos_detected_at__isnull=False,
                          sos_detected_at__lte=target.sos_detected_at,
                          sos_closed_at__isnull=True)
                  .update(sos_closed_at=timezone.now()))

        logger.info(f"[sos] 사건 종결 device={device_id} session_id={session_id} closed={closed}")
        return JsonResponse({"ok": True, "closed": closed})
