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
from imu.services import run_imu_level_for_window, run_imu_overlap_for_window


SensorWindowResponseSerializer = inline_serializer(
    name="SensorWindowResponse",
    fields={
        "status": serializers.CharField(help_text="처리 결과 상태(success/fail)"),
        "device_id": serializers.CharField(),
        "received_window_count": serializers.IntegerField(help_text="현재 세션에서 수신된 윈도우 개수"),
        "mode": serializers.IntegerField(help_text="1=THREAT, 2=PERIODIC, 3=캘리브레이션"),
        "imu": serializers.DictField(
            allow_null=True,
            help_text="이번 윈도우 전체(12초)에 대한 IMU 계산 결과",
        ),
        "imu_overlap": serializers.DictField(
            allow_null=True,
            help_text="직전 윈도우 뒤 6초 + 이번 윈도우 앞 6초를 이어붙인 오버랩 구간 IMU 계산 결과. "
                       "직전 윈도우가 없거나(세션 첫 윈도우) 연속되지 않으면 imu_status=skipped",
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
- 3: 12초 윈도우 최대 25개(=5분). idx를 함께 보내면 25에 도달한 순간 세션이 즉시 종료됨.
  idx 없이 8개만 보내고 멈추는 경우(96초 캘리브레이션)에는 즉시 종료되지 않고,
  타임아웃(90초 무응답) 도달 시 백그라운드 작업(close_stale_sessions 커맨드)이 종료 처리함

Request body:
- device_id: string, 전용 워치 ID
- mode: number, 1=이벤트보고, 2=주기보고, 3=Calibration
- sample_rate_hz: integer, 25Hz 고정
- duration_sec: integer, 보내는 값 그대로 사용(고정값 아님, 기본 12)
- timestamp: 해당 12초 윈도우의 시작 시간. UNIX time, ms
- idx: integer, 데이터 윈도우 인덱스. 5분간 12초 윈도우 전송 시 1~25 순차 번호. THREAT/PERIODIC(1/2)에서 필수, Calibration(3)에서는 불필요
- imu.x: number[], 길이 300 기준(300개 초과 시 앞 300개만 사용, 15개 넘게 초과하면 에러)
- imu.y: number[], 길이 300 기준(300개 초과 시 앞 300개만 사용, 15개 넘게 초과하면 에러)
- imu.z: number[], 길이 300 기준(300개 초과 시 앞 300개만 사용, 15개 넘게 초과하면 에러)
- ppg_green: number[], 길이 300 기준(300개 초과 시 앞 300개만 사용, 15개 넘게 초과하면 에러)

IMU 계산 시점:
- 세션의 첫 윈도우는 12초 전체로 계산 (imu)
- 그 다음부터는 매 윈도우 도착마다 "직전 윈도우 뒤 6초 + 이번 윈도우 앞 6초" 오버랩도 함께 계산 (imu_overlap)
- 즉 결과는 첫 윈도우만 12초 뒤에 나오고, 이후로는 6초 간격을 대표하는 결과가 함께 반환됨
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
                "idx": 1,
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
                "idx": 1,
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
                "imu_overlap": {
                    "imu_status": "saved",
                    "result_id": 2,
                    "level": 2,
                    "probs": [0.1, 0.2, 0.3, 0.2, 0.2],
                    "timestamp": "2026-08-12T11:19:51",
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

    if session.mode == MonitoringSession.Mode.CALIBRATION:
        idx = serializer.validated_data.get("idx")
        if idx is not None:
            # idx를 보내는 클라이언트는 자신이 몇 번째 윈도우를 보내는지 알고 있으므로
            # DB 카운트보다 idx를 신뢰해 25 도달 시 즉시 세션을 종료할 수 있게 한다.
            window_count = idx
        else:
            window_count = SensorWindow.objects.filter(session=session).count()
        required_window_count = 25
    else:
        window_count = serializer.validated_data["idx"]
        required_window_count = 25

    update_fields = ["window_count", "last_received_at"]
    session.window_count = window_count
    session.last_received_at = window.created_at

    if window_count >= required_window_count:
        session.ended_at = timezone.now()
        update_fields.append("ended_at")

    session.save(update_fields=update_fields)

    imu_result = None
    imu_overlap_result = None
    if session.mode != MonitoringSession.Mode.CALIBRATION:
        # 오버랩(직전 윈도우 뒤 6초 + 이번 윈도우 앞 6초)이 시간상 더 앞선 구간이므로
        # 전체 윈도우 계산보다 먼저 실행해 IMU 히스테리시스 상태가 시간 순서대로 갱신되게 한다.
        imu_overlap_result = run_imu_overlap_for_window(window)
        imu_result = run_imu_level_for_window(window)

    return Response(
        {
            "status": "success",
            "device_id": protectee.device_id,
            "received_window_count": window_count,
            "mode": mode_code,
            "imu": imu_result,
            "imu_overlap": imu_overlap_result,
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