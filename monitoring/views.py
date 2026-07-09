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
    MODE_CODE_TO_VALUE,
    MODE_VALUE_TO_CODE,
    MonitoringSessionSerializer,
    SensorWindowCreateSerializer,
)
from .utils import (
    get_or_create_protectee_by_device_id,
    get_or_create_session_for_sensor_data,
)
from imu.services import run_imu_level_for_window


SensorWindowResponseSerializer = inline_serializer(
    name="SensorWindowResponse",
    fields={
        "status": serializers.CharField(help_text="처리 결과 상태(success/fail)"),
        "device_id": serializers.CharField(),
        "received_window_count": serializers.IntegerField(help_text="현재 세션에서 수신된 윈도우 개수"),
        "mode": serializers.IntegerField(help_text="1=THREAT, 2=PERIODIC, 3=캘리브레이션"),
        "imu": serializers.DictField(
            allow_null=True,
        ),
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
- 1 (이벤트보고): 데이터 저장. IMU x, y, z + PPG green 필요
- 2 (주기보고): 데이터 저장. IMU x, y, z + PPG green 필요
- 3 (Calibration): 캘리브레이션 데이터 저장. PPG green만 필요, IMU는 저장하지 않음
- 1 / 2: 12초 윈도우 25개 = 5분
- 3: 12초 윈도우 8개 = 96초

Request body:
- device_id: string, 전용 워치 ID
- mode: number, 1=이벤트보고, 2=주기보고, 3=Calibration
- sample_rate_hz: integer, 25Hz 고정
- duration_sec: integer, 12초 고정
- timestamp: 해당 12초 윈도우의 시작 시간. UNIX time, ms
- imu.x: number[], 길이 300
- imu.y: number[], 길이 300
- imu.z: number[], 길이 300
- ppg_green: number[], 길이 300
""",
    request=SensorWindowCreateSerializer,
    responses={
        201: OpenApiResponse(
            response=SensorWindowResponseSerializer,
            description="센서 데이터 저장 성공",
        ),
        400: OpenApiResponse(
            response=SensorWindowResponseSerializer,
            description="요청값 오류",
        ),
    },
    examples=[
        OpenApiExample(
            "이벤트보고(mode=1) 요청 예시",
            value={
                "device_id": "P002",
                "mode": 1,
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "imu": {
                    "x": [0.01, 0.02, 0.03],
                    "y": [0.11, 0.12, 0.13],
                    "z": [9.80, 9.79, 9.81],
                },
                "ppg_green": [12345, 12347, 12340],
            },
            request_only=True,
        ),
        OpenApiExample(
            "주기보고(mode=2) 요청 예시",
            value={
                "device_id": "P002",
                "mode": 2,
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "imu": {
                    "x": [0.01, 0.02, 0.03],
                    "y": [0.11, 0.12, 0.13],
                    "z": [9.80, 9.79, 9.81],
                },
                "ppg_green": [12345, 12347, 12340],
            },
            request_only=True,
        ),
        OpenApiExample(
            "Calibration(mode=3) 요청 예시",
            value={
                "device_id": "P002",
                "mode": 3,
                "sample_rate_hz": 25,
                "duration_sec": 12,
                "timestamp": 1777824000000,
                "ppg_green": [12345, 12347, 12340],
            },
            request_only=True,
        ),
        OpenApiExample(
            "저장 성공 응답",
            value={
                "status": "success",
                "device_id": "P002",
                "received_window_count": 1,
                "mode": 1,
                "imu": {
                    "imu_status": "saved",
                    "result_id": 1,
                    "level": 2,
                    "probs": [0.1, 0.2, 0.3, 0.2, 0.2],
                },
            },
            response_only=True,
            status_codes=["201"],
        ),
        OpenApiExample(
            "처리 실패 응답",
            value={
                "status": "fail",
                "device_id": "P002",
                "received_window_count": 0,
                "mode": 1,
                "detail": "세션을 생성하거나 찾을 수 없습니다.",
            },
            response_only=True,
            status_codes=["400"],
        ),
        OpenApiExample(
            "요청값 검증 실패 응답",
            value={
                "status": "fail",
                "device_id": "P002",
                "received_window_count": 0,
                "mode": 0,
                "detail": {
                    "imu": ["25Hz, 12초 데이터는 imu.x, imu.y, imu.z 배열 길이가 모두 300개여야 합니다."]
                },
            },
            response_only=True,
            status_codes=["400"],
        ),
    ],
)
@api_view(["POST"])
def create_sensor_window(request):
    serializer = SensorWindowCreateSerializer(data=request.data)

    if not serializer.is_valid():
        raw_device_id = request.data.get("device_id")
        raw_mode = request.data.get("mode")

        return Response(
            {
                "status": "fail",
                "device_id": raw_device_id if isinstance(raw_device_id, str) else "",
                "received_window_count": 0,
                "mode": raw_mode if raw_mode in MODE_CODE_TO_VALUE else 0,
                "detail": serializer.errors,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    device_id = serializer.validated_data["device_id"]
    mode = serializer.validated_data["mode"]
    mode_code = MODE_VALUE_TO_CODE[mode]

    try:
        protectee = get_or_create_protectee_by_device_id(device_id)
    except ValueError as e:
        return Response(
            {
                "status": "fail",
                "device_id": device_id,
                "received_window_count": 0,
                "mode": mode_code,
                "detail": str(e),
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    session, error_response = get_or_create_session_for_sensor_data(
        protectee=protectee,
        mode=mode,
        new_started_at=serializer.validated_data["started_at"],
    )

    if error_response:
        return Response(
            {
                "status": "fail",
                "device_id": protectee.device_id,
                "received_window_count": 0,
                "mode": mode_code,
                **error_response,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    if not session:
        return Response(
            {
                "status": "fail",
                "device_id": protectee.device_id,
                "received_window_count": 0,
                "mode": mode_code,
                "detail": "세션을 생성하거나 찾을 수 없습니다.",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    window = SensorWindow.objects.create(
        session=session,
        protectee=protectee,
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

    update_fields = ["window_count", "last_received_at"]
    session.window_count = window_count
    session.last_received_at = window.created_at

    if window_count >= required_window_count:
        session.ended_at = timezone.now()
        update_fields.append("ended_at")

    session.save(update_fields=update_fields)

    imu_result = None
    if session.mode != MonitoringSession.Mode.CALIBRATION:
        imu_result = run_imu_level_for_window(window)

    return Response(
        {
            "status": "success",
            "device_id": protectee.device_id,
            "received_window_count": window_count,
            "mode": mode_code,
            "imu": imu_result,
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