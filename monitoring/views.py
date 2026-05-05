from django.utils import timezone
from rest_framework import status, serializers
from rest_framework.decorators import api_view
from rest_framework.response import Response
from drf_spectacular.utils import (
    extend_schema,
    OpenApiExample,
    OpenApiResponse,
    inline_serializer,
)
from .models import MonitoringSession, SensorWindow
from .serializers import (
    MonitoringSessionSerializer,
    SensorWindowCreateSerializer,
)
from .utils import (
    get_or_create_protectee_by_device_id,
    get_or_create_session_for_sensor_data,
)


SensorWindowResponseSerializer = inline_serializer(
    name="SensorWindowResponse",
    fields={
        "message": serializers.CharField(),
        "device_id": serializers.CharField(),
        "mode": serializers.CharField(),
    },
)


ErrorResponseSerializer = inline_serializer(
    name="ErrorResponse",
    fields={
        "detail": serializers.CharField(),
    },
)


@extend_schema(
    tags=["monitoring"],
    summary="센서 데이터 수신",
    description="""
25Hz, 12초 단위 센서 데이터 수신

mode별 처리:
- THREAT: 이벤트보고 데이터 저장. IMU x, y, z + PPG green 필요
- PERIODIC: 주기보고 데이터 저장. IMU x, y, z + PPG green 필요
- CALIBRATION: 캘리브레이션 데이터 저장. PPG green만 필요, IMU는 저장하지 않음
- THREAT / PERIODIC: 12초 윈도우 25개 = 5분
- CALIBRATION: 12초 윈도우 8개 = 96초

Request body:
- device_id: string, 전용 워치 ID
- mode: string, THREAT 또는 PERIODIC 또는 CALIBRATION
- sample_rate_hz: integer, 25Hz 고정
- duration_sec: integer, 12초 고정
- timestamp: 해당 12초 윈도우의 시작 시간. UNIX time, ms
- imu.x: number[], 길이 300
- imu.y: number[], 길이 300
- imu.z: number[], 길이 300
- ppg.green: number[], 길이 300
""",
    request=SensorWindowCreateSerializer,
    responses={
        201: OpenApiResponse(
            response=SensorWindowResponseSerializer,
            description="센서 데이터 저장 성공",
        ),
        400: OpenApiResponse(
            response=ErrorResponseSerializer,
            description="요청값 오류",
        ),
    },
    examples=[
        OpenApiExample(
            "THREAT 요청 예시",
            value={
                "device_id": "P002",
                "mode": "THREAT",
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "imu": {
                    "x": [0.01, 0.02, 0.03],
                    "y": [0.11, 0.12, 0.13],
                    "z": [9.80, 9.79, 9.81],
                },
                "ppg": {
                    "green": [12345, 12347, 12340],
                },
            },
            request_only=True,
        ),
        OpenApiExample(
            "PERIODIC 요청 예시",
            value={
                "device_id": "P002",
                "mode": "PERIODIC",
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "imu": {
                    "x": [0.01, 0.02, 0.03],
                    "y": [0.11, 0.12, 0.13],
                    "z": [9.80, 9.79, 9.81],
                },
                "ppg": {
                    "green": [12345, 12347, 12340],
                },
            },
            request_only=True,
        ),
        OpenApiExample(
            "CALIBRATION 요청 예시",
            value={
                "device_id": "P002",
                "mode": "CALIBRATION",
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "ppg": {
                    "green": [12345, 12347, 12340],
                },
            },
            request_only=True,
        ),
        OpenApiExample(
            "저장 성공 응답",
            value={
                "message": "센서 데이터가 저장되었습니다.",
                "device_id": "P002",
                "mode": "THREAT",
            },
            response_only=True,
            status_codes=["201"],
        ),
        OpenApiExample(
            "캘리브레이션 완료 응답",
            value={
                "message": "캘리브레이션이 완료되었습니다.",
                "device_id": "P002",
                "mode": "CALIBRATION",
            },
            response_only=True,
            status_codes=["201"],
        ),
    ],
)
@api_view(["POST"])
def create_sensor_window(request):
    serializer = SensorWindowCreateSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    device_id = serializer.validated_data["device_id"]
    mode = serializer.validated_data["mode"]

    try:
        protectee = get_or_create_protectee_by_device_id(device_id)
    except ValueError as e:
        return Response(
            {"detail": str(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )

    session, error_response = get_or_create_session_for_sensor_data(
        protectee=protectee,
        mode=mode,
    )

    if error_response:
        return Response(
            error_response,
            status=status.HTTP_400_BAD_REQUEST,
        )

    if not session:
        return Response(
            {"detail": "세션을 생성하거나 찾을 수 없습니다."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    SensorWindow.objects.create(
        session=session,
        sample_rate_hz=serializer.validated_data["sample_rate_hz"],
        duration_sec=serializer.validated_data["duration_sec"],
        started_at=serializer.validated_data["started_at"],
        x=serializer.validated_data["x"],
        y=serializer.validated_data["y"],
        z=serializer.validated_data["z"],
        ppg_green=serializer.validated_data["ppg_green"],
    )

    window_count = SensorWindow.objects.filter(
        session=session,
    ).count()

    if session.mode == MonitoringSession.Mode.CALIBRATION:
        required_window_count = 8
    else:
        required_window_count = 25

    session_completed = window_count >= required_window_count

    message = "센서 데이터가 저장되었습니다."

    if session_completed:
        session.ended_at = timezone.now()
        session.save(update_fields=["ended_at"])

        if session.mode == MonitoringSession.Mode.PERIODIC:
            message = "주기보고가 완료되었습니다."

        elif session.mode == MonitoringSession.Mode.THREAT:
            message = "이벤트보고 데이터 수집이 완료되었습니다."

        elif session.mode == MonitoringSession.Mode.CALIBRATION:
            message = "캘리브레이션이 완료되었습니다."

    return Response(
        {
            "message": message,
            "device_id": protectee.device_id,
            "mode": session.mode,
        },
        status=status.HTTP_201_CREATED,
    )


@extend_schema(
    tags=["monitoring"],
    summary="모니터링 세션 목록 조회",
    description="개발 확인용 API입니다.",
    responses={
        200: OpenApiResponse(
            response=MonitoringSessionSerializer(many=True),
            description="조회 성공",
        )
    },
)
@api_view(["GET"])
def session_list(request):
    sessions = MonitoringSession.objects.select_related("protectee").all()
    serializer = MonitoringSessionSerializer(sessions, many=True)
    return Response(serializer.data)


@extend_schema(
    tags=["monitoring"],
    summary="모니터링 세션 상세 조회",
    description="개발 확인용 API입니다.",
    responses={
        200: OpenApiResponse(
            response=MonitoringSessionSerializer,
            description="조회 성공",
        ),
        404: OpenApiResponse(
            response=ErrorResponseSerializer,
            description="세션 없음",
        ),
    },
)
@api_view(["GET"])
def session_detail(request, session_id):
    try:
        session = MonitoringSession.objects.select_related("protectee").get(id=session_id)
    except MonitoringSession.DoesNotExist:
        return Response(
            {"detail": "해당 세션을 찾을 수 없습니다."},
            status=status.HTTP_404_NOT_FOUND,
        )

    serializer = MonitoringSessionSerializer(session)
    return Response(serializer.data)