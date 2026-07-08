import json
from datetime import datetime, timedelta, timezone as dt_timezone
from pathlib import Path

from django.conf import settings
from django.db import transaction
from django.utils.dateparse import parse_datetime
from drf_spectacular.utils import extend_schema, OpenApiExample, OpenApiParameter
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from monitoring.models import Protectee
from .models import GeoData, GeoProcessedData, GeoTripAnomalyResult
from .serializers import (
    GeoDataIngestSerializer,
    GeoDataIngestResponseSerializer,
    GeoTrackResponseSerializer,
)
from .gpr_services import create_geo_processed_data_and_run_gpr

KST = dt_timezone(timedelta(hours=9))

_BASELINE_DIR = Path(settings.BASE_DIR) / "media" / "models" / "geo" / "baseline"


def _load_baseline_data(device_id, od_key=None):
    """
    Baseline JSON 파일에서 경로 데이터와 앵커 존 로드.
    od_key가 주어지면 해당 OD만, 없으면 전체 baseline 반환.
    """
    baseline_routes = []
    anchors = []

    if not _BASELINE_DIR.exists():
        return baseline_routes, anchors

    for trip_file in sorted(_BASELINE_DIR.glob("baseline_trip_points_*.json")):
        try:
            raw = json.loads(trip_file.read_text(encoding="utf-8"))
            raw = [p for p in raw if p.get("device_id") == device_id]
            matched = [p for p in raw if p.get("od_key") == od_key] if od_key else raw

            trips = {}
            for p in matched:
                tid = p["trip_id"]
                if tid not in trips:
                    trips[tid] = {"trip_id": tid, "od_key": p.get("od_key"), "points": []}
                trips[tid]["points"].append({"lat": p["Latitude"], "lon": p["Longitude"]})

            baseline_routes.extend(trips.values())
        except Exception:
            pass

    for anchor_file in sorted(_BASELINE_DIR.glob("baseline_anchor_zones_*.json")):
        try:
            raw = json.loads(anchor_file.read_text(encoding="utf-8"))
            anchors = [
                {
                    "anchor_id": a["anchor_id"],
                    "lat": a["anchor_lat"],
                    "lon": a["anchor_lon"],
                    "radius_m": a["anchor_radius_m"],
                }
                for a in raw
                if a.get("device_id") == device_id
            ]
        except Exception:
            pass

    return baseline_routes, anchors


class GeoDataIngestView(APIView):
    """
    POST /api/geo/data
    """

    @extend_schema(
        request=GeoDataIngestSerializer,
        responses={201: GeoDataIngestResponseSerializer},
        summary="GEO 위치 데이터 수신",
        description=(
            "실시간 위치 정보를 수신하는 API입니다.\n\n"
        ),
        examples=[
            OpenApiExample(
                name="성공 케이스",
                value={
                    "device_id": "5456a4dfb33d71d5",
                    "locations": [
                        {
                            "timestamp": 1672531200000,
                            "pos_success": True,
                            "pos_info": {
                                "longitude": 126.9780,
                                "latitude": 37.5665,
                                "accuracy_h": 5.5
                            }
                        }
                    ]
                },
                request_only=True,
            ),
            OpenApiExample(
                name="실패 케이스",
                value={
                    "device_id": "5456a4dfb33d71d5",
                    "locations": [
                        {
                            "timestamp": 1672531200000,
                            "pos_success": False
                        }
                    ]
                },
                request_only=True,
            ),
        ],
    )
    def post(self, request):
        serializer = GeoDataIngestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        device_id = data["device_id"]

        # GeoData 원본 저장
        with transaction.atomic():
            protectee, _ = Protectee.objects.get_or_create(
                device_id=device_id,
                defaults={"name": f"unknown-{device_id[:6]}"},
            )

            location_items = []
            for item in data["locations"]:
                pos_success = item["pos_success"]
                pos_info = item.get("pos_info")

                GeoData.objects.create(
                    protectee=protectee,
                    device_id=device_id,
                    timestamp=item["timestamp"],
                    pos_success=pos_success,
                    longitude=pos_info["longitude"] if pos_success and pos_info else None,
                    latitude=pos_info["latitude"] if pos_success and pos_info else None,
                    accuracy_h=pos_info["accuracy_h"] if pos_success and pos_info else None,
                )
                location_items.append((item, pos_success, pos_info, protectee))

        # GPR 보정 + 이상탐지 (트랜잭션 밖에서 실행)
        gpr_results = []
        for item, pos_success, pos_info, protectee in location_items:
            latitude = pos_info["latitude"] if pos_success and pos_info else None
            longitude = pos_info["longitude"] if pos_success and pos_info else None

            _, gpr_result, anomaly_result = create_geo_processed_data_and_run_gpr(
                protectee=protectee,
                device_id=device_id,
                timestamp=item["timestamp"],
                latitude=latitude,
                longitude=longitude,
            )
            gpr_results.append({
                "timestamp": item["timestamp"].isoformat(),
                "gpr_status": gpr_result.get("gpr_status"),
                "anomaly_status": anomaly_result.get("anomaly_status"),
            })

        response_data = {
            "status": "ok",
            "saved_count": len(location_items),
            "gpr_results": gpr_results,
        }

        return Response(response_data, status=status.HTTP_201_CREATED)


def _parse_query_datetime(value):
    """
    쿼리 파라미터로 들어온 시각 문자열을 naive KST datetime으로 변환.
    ISO 8601 문자열과 UNIX time(ms) 정수 문자열을 모두 지원한다.
    """
    if not value:
        return None

    if value.isdigit():
        return datetime.fromtimestamp(int(value) / 1000.0, tz=KST).replace(tzinfo=None)

    parsed = parse_datetime(value)
    if parsed is not None and parsed.tzinfo is not None:
        parsed = parsed.astimezone(KST).replace(tzinfo=None)
    return parsed


class GeoTrackDataView(APIView):
    """
    GET /api/geo/track

    지도 시각화용 위치 트랙(점 + 선) 데이터를 조회한다.
    GeoProcessedData의 최종 보정 좌표(latitude/longitude)를 기준으로 반환한다.
    """

    @extend_schema(
        parameters=[
            OpenApiParameter("device_id", str, required=True, description="워치 고유 ID"),
            OpenApiParameter("start", str, required=False, description="조회 시작 시각 (ISO 8601 또는 UNIX time ms)"),
            OpenApiParameter("end", str, required=False, description="조회 종료 시각 (ISO 8601 또는 UNIX time ms)"),
            OpenApiParameter("window_minutes", int, required=False, description="가장 최근 데이터 시각 기준 N분 전부터 조회 (start 미지정 시 사용)"),
            OpenApiParameter("limit", int, required=False, description="최대 반환 개수 (기본 5000)"),
        ],
        responses={200: GeoTrackResponseSerializer},
        summary="GEO 위치 트랙 조회",
    )
    def get(self, request):
        device_id = request.query_params.get("device_id")
        if not device_id:
            return Response({"detail": "device_id는 필수입니다."}, status=status.HTTP_400_BAD_REQUEST)

        qs = GeoProcessedData.objects.filter(
            device_id=device_id,
            latitude__isnull=False,
            longitude__isnull=False,
        )

        start = _parse_query_datetime(request.query_params.get("start"))
        end = _parse_query_datetime(request.query_params.get("end"))
        window_minutes = request.query_params.get("window_minutes")

        if start is None and window_minutes:
            latest = qs.order_by("-timestamp").values_list("timestamp", flat=True).first()
            if latest is not None:
                try:
                    minutes = float(window_minutes)
                except (TypeError, ValueError):
                    minutes = None
                if minutes is not None:
                    start = latest - timedelta(minutes=minutes)

        if start is not None:
            qs = qs.filter(timestamp__gte=start)
        if end is not None:
            qs = qs.filter(timestamp__lte=end)

        try:
            limit = min(int(request.query_params.get("limit", 5000)), 5000)
        except (TypeError, ValueError):
            limit = 5000

        rows = list(
            qs.order_by("timestamp").values(
                "id", "timestamp", "latitude", "longitude", "gps_quality", "state_primary"
            )[:limit]
        )

        serializer = GeoTrackResponseSerializer(
            {"device_id": device_id, "count": len(rows), "points": rows}
        )
        return Response(serializer.data, status=status.HTTP_200_OK)


class GeoTripMapView(APIView):
    """
    GET /api/geo/trip_map/?device_id=...

    최신 여정 이상탐지 결과 + 현재 경로 GPS + 베이스라인 경로 + 앵커존을 반환한다.
    final_route_label에 따른 지도 시각화용 API.
    """

    def get(self, request):
        device_id = request.query_params.get("device_id")
        if not device_id:
            return Response({"detail": "device_id 필수"}, status=status.HTTP_400_BAD_REQUEST)

        trip_id = request.query_params.get("trip_id")
        if trip_id:
            anomaly_obj = (
                GeoTripAnomalyResult.objects
                .filter(id=trip_id, device_id=device_id)
                .first()
            )
        else:
            anomaly_obj = (
                GeoTripAnomalyResult.objects
                .filter(device_id=device_id)
                .order_by("-trip_end_time")
                .first()
            )

        od_key = anomaly_obj.od_key if anomaly_obj else None

        # 현재 trip GPS 포인트 (anomaly result의 시간 구간 내)
        current_route = []
        if anomaly_obj:
            rows = (
                GeoProcessedData.objects
                .filter(
                    device_id=device_id,
                    timestamp__gte=anomaly_obj.trip_start_time,
                    timestamp__lte=anomaly_obj.trip_end_time,
                    latitude__isnull=False,
                    longitude__isnull=False,
                )
                .order_by("timestamp")
                .values("timestamp", "latitude", "longitude", "state_primary")
            )
            current_route = [
                {
                    "lat": r["latitude"],
                    "lon": r["longitude"],
                    "timestamp": r["timestamp"].isoformat(),
                    "state": r["state_primary"],
                }
                for r in rows
            ]

        baseline_routes, anchors = _load_baseline_data(device_id, od_key)

        anomaly_data = None
        if anomaly_obj:
            anomaly_data = {
                "final_route_label": anomaly_obj.final_route_label,
                "od_key": anomaly_obj.od_key,
                "trip_start_time": anomaly_obj.trip_start_time.isoformat(),
                "trip_end_time": anomaly_obj.trip_end_time.isoformat(),
                "dtw_score": anomaly_obj.dtw_score,
                "threshold": anomaly_obj.threshold,
                "message": anomaly_obj.message,
            }

        return Response({
            "anomaly_result": anomaly_data,
            "current_route": current_route,
            "baseline_routes": list(baseline_routes),
            "anchors": anchors,
        })


class GeoTripHistoryView(APIView):
    """
    GET /api/geo/trip_history/?device_id=...&date=YYYY-MM-DD(KST)
    여정 이상탐지 결과 목록을 반환한다.
    """

    def get(self, request):
        device_id = request.query_params.get("device_id")
        if not device_id:
            return Response({"detail": "device_id 필수"}, status=status.HTTP_400_BAD_REQUEST)

        date_str = request.query_params.get("date")  # YYYY-MM-DD (KST)

        qs = (
            GeoTripAnomalyResult.objects
            .filter(device_id=device_id)
            .order_by("-trip_end_time")
        )

        if date_str:
            try:
                day_start = datetime.strptime(date_str, "%Y-%m-%d")
                day_end = day_start + timedelta(days=1)
                qs = qs.filter(trip_end_time__gte=day_start, trip_end_time__lt=day_end)
            except ValueError:
                pass

        trips = [
            {
                "id": obj.id,
                "od_key": obj.od_key,
                "final_route_label": obj.final_route_label,
                "trip_start_time": obj.trip_start_time.isoformat(),
                "trip_end_time": obj.trip_end_time.isoformat(),
                "dtw_score": obj.dtw_score,
                "threshold": obj.threshold,
            }
            for obj in qs[:50]
        ]

        return Response({"trips": trips})


class GeoTripCalendarView(APIView):
    """
    GET /api/geo/trip_calendar/?device_id=...&year=YYYY&month=M
    해당 월에 여정이 존재하는 KST 날짜 목록을 반환한다.
    """

    def get(self, request):
        device_id = request.query_params.get("device_id")
        if not device_id:
            return Response({"detail": "device_id 필수"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            year  = int(request.query_params.get("year",  0))
            month = int(request.query_params.get("month", 0))
            if not (1 <= month <= 12):
                raise ValueError
        except (TypeError, ValueError):
            return Response({"detail": "year, month 필수 (1-12)"}, status=status.HTTP_400_BAD_REQUEST)

        month_start = datetime(year, month, 1)
        month_end   = datetime(year + 1, 1, 1) if month == 12 \
                      else datetime(year, month + 1, 1)

        end_times = (
            GeoTripAnomalyResult.objects
            .filter(device_id=device_id,
                    trip_end_time__gte=month_start,
                    trip_end_time__lt=month_end)
            .values_list("trip_end_time", flat=True)
        )

        dates = sorted({
            t.strftime("%Y-%m-%d")
            for t in end_times
        })

        return Response({"dates_with_trips": dates})