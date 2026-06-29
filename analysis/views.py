from rest_framework import status, serializers
from rest_framework.decorators import api_view
from rest_framework.response import Response

from drf_spectacular.utils import (
    extend_schema,
    OpenApiExample,
    OpenApiResponse,
    inline_serializer,
)

from monitoring.utils import get_or_create_protectee_by_device_id
from .models import Result
from .serializers import ResultCreateSerializer, ResultSerializer


ResultSaveResponseSerializer = inline_serializer(
    name="ResultSaveResponse",
    fields={
        "message": serializers.CharField(),
        "result_id": serializers.IntegerField(),
        "device_id": serializers.CharField(),
        "mode": serializers.CharField(),
        "event_type": serializers.CharField(),
    },
)

ErrorResponseSerializer = inline_serializer(
    name="AnalysisErrorResponse",
    fields={
        "detail": serializers.CharField(),
    },
)


@extend_schema(
    tags=["analysis"],
    summary="PPG/IMU 결과 저장",
    description="""
PPG/IMU 위험 분석 결과 저장

Request body:
- device_id: string, 전용 워치 ID
- mode: string, THREAT, PERIODIC 또는 CALIBRATION
- event_type: string, PPG, IMU 또는 GEO
- timestamp_ms: integer, UNIX time, 위험 분석에 사용한 데이터 구간의 끝 시간
- probability: number | null, 위험 확률
- risk_level: integer | null, 1~5 위험도 등급
- risk_detected: boolean | null, 위험 감지 여부

처리:
- 결과만 저장
- 상시보고 종료는 /api/monitoring/threat/stop/ API에서 처리
""",
    request=ResultCreateSerializer,
    responses={
        201: OpenApiResponse(
            response=ResultSaveResponseSerializer,
            description="결과 저장 성공",
        ),
        400: OpenApiResponse(
            response=ErrorResponseSerializer,
            description="요청값 오류",
        ),
    },
    examples=[
        OpenApiExample(
            "PPG 결과 저장 요청",
            value={
                "device_id": "P002",
                "mode": "THREAT",
                "event_type": "PPG",
                "timestamp": 1777824330000,
                "probability": 0.82,
                "risk_level": 4,
                "risk_detected": True,
            },
            request_only=True,
        ),
        OpenApiExample(
            "IMU 결과 저장 요청",
            value={
                "device_id": "P002",
                "mode": "THREAT",
                "event_type": "IMU",
                "timestamp": 1777824330000,
                "probability": None,
                "risk_level": 1,
                "risk_detected": False,
            },
            request_only=True,
        ),
        OpenApiExample(
            "결과값 없음 요청",
            value={
                "device_id": "P002",
                "mode": "THREAT",
                "event_type": "PPG",
                "timestamp": 1777824330000,
                "probability": None,
                "risk_level": None,
                "risk_detected": None,
            },
            request_only=True,
        ),
        OpenApiExample(
            "결과 저장 응답",
            value={
                "message": "결과가 저장되었습니다.",
                "result_id": 1,
                "device_id": "P002",
                "mode": "THREAT",
                "event_type": "PPG",
            },
            response_only=True,
            status_codes=["201"],
        ),
    ],
)
@api_view(["POST"])
def create_result(request):
    serializer = ResultCreateSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    device_id = serializer.validated_data["device_id"]

    try:
        protectee = get_or_create_protectee_by_device_id(device_id)
    except ValueError as e:
        return Response(
            {"detail": str(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )

    result = Result.objects.create(
        device_id=protectee.device_id,
        mode=serializer.validated_data["mode"],
        event_type=serializer.validated_data["event_type"],
        timestamp=serializer.validated_data["timestamp"],
        probability=serializer.validated_data.get("probability"),
        risk_level=serializer.validated_data.get("risk_level"),
        risk_detected=serializer.validated_data.get("risk_detected"),
    )

    return Response(
        {
            "message": "결과가 저장되었습니다.",
            "result_id": result.id,
            "device_id": result.device_id,
            "mode": result.mode,
            "event_type": result.event_type,
        },
        status=status.HTTP_201_CREATED,
    )


@extend_schema(
    tags=["analysis"],
    summary="PPG/IMU 결과 목록 조회",
    description="""
개발 확인용 API입니다.

Query parameter:
- device_id: string, 선택값
- mode: string, 선택값, THREAT, PERIODIC 또는 CALIBRATION
- event_type: string, 선택값, PPG, IMU 또는 GEO
""",
    responses={
        200: OpenApiResponse(
            response=ResultSerializer(many=True),
            description="조회 성공",
        )
    },
)
@api_view(["GET"])
def result_list(request):
    results = Result.objects.all().order_by("-created_at")

    device_id = request.query_params.get("device_id")
    mode = request.query_params.get("mode")
    event_type = request.query_params.get("event_type")

    if device_id:
        results = results.filter(device_id=device_id)

    if mode:
        results = results.filter(mode=mode)

    if event_type:
        results = results.filter(event_type=event_type)

    serializer = ResultSerializer(results, many=True)
    return Response(serializer.data)