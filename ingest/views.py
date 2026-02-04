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
            "요청 규칙\n"
            "- device_id (required)\n"
            "- samples (required)\n"
            "- sample 필수 필드: time, ax, ay, az, ppg_green\n"
            "- ppg_ir/ppg_red는 optional이며 없으면 생략하거나 null로 전송 가능\n"
            "- window_sec=6, hz=25\n\n"
            # "서버 저장 규칙\n"
            # "- seq는 서버가 수신 순서대로 0..N-1 자동 부여\n"
            # "- t_start/t_end는 samples[0].time / samples[-1].time 값으로 저장\n"
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
                "Request example (with optional fields)",
                value={
                    "device_id": "SM-L300_ABC123",
                    "sos_id": "SOS_20260130_0001",
                    "window_sec": 6,
                    "hz": 25,
                    "samples": [
                        {"time": "00:00:00.000", "ax": 0.186416, "ay": 0.066368, "az": -0.93696, "ppg_green": 37457, "ppg_ir": None, "ppg_red": None},
                        {"time": "00:00:00.040", "ax": 0.173728, "ay": 0.121024, "az": -0.93696, "ppg_green": 45171}
                    ],
                },
                description="실제 샘플 수는 클라이언트 구현에 따라 달라질 수 있습니다.",
                request_only=True,
            ),
            OpenApiExample(
                "Request example (minimal)",
                value={
                    "device_id": "SM-L300_ABC123",
                    "samples": [
                        {
                            "time": "00:00:00.000",
                            "ax": 0.186416,
                            "ay": 0.066368,
                            "az": -0.93696,
                            "ppg_green": 37457
                        }
                    ],
                },
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
