from datetime import datetime, timedelta, timezone as dt_timezone

from django.db import transaction
from django.utils.dateparse import parse_datetime
from drf_spectacular.utils import extend_schema, OpenApiExample, OpenApiParameter
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from monitoring.models import Protectee
from .models import GeoData, GeoProcessedData
from .serializers import (
    GeoDataIngestSerializer,
    GeoDataIngestResponseSerializer,
    GeoTrackResponseSerializer,
)
from .gpr_services import create_geo_processed_data_and_run_gpr


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
    쿼리 파라미터로 들어온 시각 문자열을 timezone-aware datetime으로 변환.
    ISO 8601 문자열과 UNIX time(ms) 정수 문자열을 모두 지원한다.
    """
    if not value:
        return None

    if value.isdigit():
        return datetime.fromtimestamp(int(value) / 1000.0, tz=dt_timezone.utc)

    parsed = parse_datetime(value)
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