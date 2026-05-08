from drf_spectacular.utils import OpenApiExample, extend_schema
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import GeoDataCreateSerializer


class GeoDataCreateAPIView(APIView):
    @extend_schema(
        tags=["geo"],
        summary="GPS 데이터 수신",
        description="""
5분 주기로 GPS 데이터 수신

Request body:
- device_id: string, 전용 워치 ID
- timestamp: UNIX time, ms
- latitude: double, 위도
- longitude: double, 경도
        """,
        request=GeoDataCreateSerializer,
        responses={
            201: {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "example": "geo data saved",
                    },
                },
            }
        },
        examples=[
            OpenApiExample(
                name="GPS 데이터 수신 예시",
                value={
                    "device_id": "5456a4dfb33d71d5",
                    "timestamp": 1714896000000,
                    "latitude": 37.123456,
                    "longitude": 127.123456,
                },
                request_only=True,
            ),
        ],
    )
    def post(self, request):
        serializer = GeoDataCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        return Response(
            {
                "message": "geo data saved",
            },
            status=status.HTTP_201_CREATED,
        )