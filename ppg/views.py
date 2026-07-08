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
                "device_id":        r.device_id,
                "timestamp":        sw.started_at.isoformat(),
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
                "device_id":        r.device_id,
                "timestamp":        sw.started_at.isoformat(),
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


class EventStatusView(View):
    def get(self, request):
        from datetime import timedelta
        from analysis.models import Result
        from django.utils import timezone

        device_id = request.GET.get("device_id")
        if not device_id:
            return JsonResponse({"ok": False, "error": "device_id required"}, status=400)

        # 3분 이내 데이터만
        three_min_ago = timezone.now() - timedelta(minutes=3)

        latest = (Result.objects
                  .filter(
                      device_id=device_id,
                      event_type=Result.EventType.IMU,
                      timestamp__gte=three_min_ago,
                  )
                  .order_by("-timestamp")
                  .first())

        if not latest:
            return JsonResponse({
                "ok":               False,
                "imu_display":      "데이터 없음",
                "imu_danger_level": None,
                "timestamp":        None,
            })

        level = latest.risk_level
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
        })