from django.db import transaction
from drf_spectacular.utils import extend_schema, OpenApiExample
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from monitoring.models import Protectee
from .models import GeoData
from .serializers import GeoDataIngestSerializer,GeoDataIngestResponseSerializer
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