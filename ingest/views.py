from django.db import transaction
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiExample
from .serializers import IngestSerializer, IngestResponseSerializer
from .models import ThreatWindow, ThreatSample


class ThreatIngestView(APIView):
    serializer_class = IngestSerializer

    @extend_schema(
        tags=["Threat"],
        summary="Threat ingest",
        description=(
            "전용 단말에서 IMU/PPG 데이터를 업로드합니다.\n\n"
            "- Method: POST\n"
            "- Content-Type: application/json\n"
            "- Endpoint: /api/threat/ingest\n\n"
            "업로드 단위\n"
            "- 1회 요청 = 1개 윈도우(window)이며, 6초 동안 수집한 샘플을 한 번에 전송합니다.\n"
            "- '6초 x 25hz = 150' 총 150개(samples=150) 입니다.\n"
            "- 따라서 6초 동안 샘플을 버퍼링한 뒤, 6초 주기로 한 번씩 업로드해야 합니다.\n\n"
            "요청 규칙\n"
            "- device_id (required)\n"
            "- samples (required): 길이=150 (window_sec=6, hz=25 기준)\n"
            "- sample 필수 필드: time, ax, ay, az, ppg_green\n"
            "- ppg_ir/ppg_red는 optional이며 없으면 생략하거나 null로 전송 가능\n"
            "- time 형식: YYYY-MM-DD HH:MM:SS.mmm (예: 2026-02-06 06:45:00.000)\n"
            "- window_sec=6, hz=25\n\n"
        ),
        request=IngestSerializer,
        responses={
            201: IngestResponseSerializer,
            400: OpenApiExample(
                "Bad Request example",
                value={"samples": "At least 1 sample is required."},
                response_only=True,
                status_codes=["400"],
            )
        },
        examples=[
            OpenApiExample(
                "Request example (6s window = 150 samples, truncated)",
                value={
                    "device_id": "SM-L300_ABC123",
                    "sos_id": "SOS_20260206_0001",
                    "window_sec": 6,
                    "hz": 25,
                    "samples": [
                        {"time": "2026-02-06 06:45:00.000", "ax": 0.186416, "ay": 0.066368, "az": -0.93696, "ppg_green": 37457},
                        {"time": "2026-02-06 06:45:00.040", "ax": 0.173728, "ay": 0.121024, "az": -0.93696, "ppg_green": 45171},
                        {"time": "2026-02-06 06:45:00.080", "ax": 0.170000, "ay": 0.120000, "az": -0.93000, "ppg_green": 44900},
                        # ... 중간 147개 생략 ...
                        {"time": "2026-02-06 06:45:05.960", "ax": 0.160000, "ay": 0.110000, "az": -0.92000, "ppg_green": 44000},
                    ],
                },
                description="samples는 실제로 총 150개(6초×25Hz)이며, 문서에는 일부만 표시했습니다.",
                request_only=True,
            ),
            OpenApiExample(
                "Response example",
                value={"ok": True, "window_id": 123, "saved_samples": 2},
                response_only=True,
                status_codes=["201"],
            ),
        ],
    )
    def post(self, request):
        serializer = IngestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        samples = data["samples"]

        t_start = samples[0]["time"]
        t_end = samples[-1]["time"]

        with transaction.atomic():
            window = ThreatWindow.objects.create(
                device_id=data["device_id"],
                sos_id=data.get("sos_id"),
                window_sec=data.get("window_sec", None),
                hz=data.get("hz", None),
                t_start=t_start,
                t_end=t_end,
                sample_count=len(samples),
            )

            objs = [
                ThreatSample(
                    window=window,
                    seq=i,
                    time=s["time"],
                    ax=s["ax"],
                    ay=s["ay"],
                    az=s["az"],
                    ppg_green=s["ppg_green"],
                    ppg_ir=s.get("ppg_ir"),
                    ppg_red=s.get("ppg_red"),
                )
                for i, s in enumerate(samples)
            ]

            ThreatSample.objects.bulk_create(objs, batch_size=500)

        return Response(
            {"ok": True, "window_id": window.id, "saved_samples": len(samples)},
            status=status.HTTP_201_CREATED,
        )
