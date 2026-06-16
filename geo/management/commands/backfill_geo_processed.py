from django.core.management.base import BaseCommand

from geo.models import GeoData, GeoProcessedData, GeoTripAnomalyResult
from geo.gpr_services import (
    GEO_MODEL_DEVICE_ID,
    create_geo_processed_data_and_run_gpr,
    fill_remaining_gaps_with_linear_interpolation,
)
from geo.anomaly_services import run_anomaly_for_latest


class Command(BaseCommand):
    help = (
        "geo_data에 쌓인 GPR/Anomaly 지원 device_id의 데이터를 시간순으로 "
        "다시 처리하여 geo_processed_data / geo_trip_anomaly_result를 채운다."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--device-id",
            default=GEO_MODEL_DEVICE_ID,
            help=f"처리할 device_id (기본값: {GEO_MODEL_DEVICE_ID})",
        )

    def handle(self, *args, **options):
        device_id = options["device_id"]

        deleted_anomaly, _ = GeoTripAnomalyResult.objects.filter(
            device_id=device_id
        ).delete()
        deleted_processed, _ = GeoProcessedData.objects.filter(
            device_id=device_id
        ).delete()

        self.stdout.write(
            f"[reset] device_id={device_id} "
            f"geo_processed_data={deleted_processed}건, "
            f"geo_trip_anomaly_result={deleted_anomaly}건 삭제"
        )

        geo_rows = GeoData.objects.filter(device_id=device_id).order_by("timestamp")
        total = geo_rows.count()

        if total == 0:
            self.stdout.write(f"device_id={device_id}에 해당하는 geo_data가 없습니다.")
            return

        saved_anomaly = 0

        for i, geo_row in enumerate(geo_rows, start=1):
            _, gpr_result, anomaly_result = create_geo_processed_data_and_run_gpr(
                protectee=geo_row.protectee,
                device_id=geo_row.device_id,
                timestamp=geo_row.timestamp,
                latitude=geo_row.latitude,
                longitude=geo_row.longitude,
            )

            if anomaly_result.get("anomaly_status") == "saved":
                saved_anomaly += 1

            self.stdout.write(
                f"[{i}/{total}] {geo_row.timestamp} "
                f"gpr={gpr_result.get('gpr_status')} "
                f"anomaly={anomaly_result.get('anomaly_status')}"
                f"({anomaly_result.get('reason', '')})"
            )

        self.stdout.write(
            f"완료: geo_processed_data {total}건 생성, "
            f"geo_trip_anomaly_result {saved_anomaly}건 저장"
        )

        # 실시간 처리에서는 이후 시점 데이터를 볼 수 없어 채워지지 못한
        # gpr_fill_needed row를, backfill이라 전체 데이터를 알고 있는 지금
        # 앞/뒤 정상 좌표 사이 선형보간으로 채운다.
        filled = fill_remaining_gaps_with_linear_interpolation(device_id)
        self.stdout.write(f"[gap-fill] 선형보간으로 {filled}건 채움")

        if filled:
            deleted_anomaly, _ = GeoTripAnomalyResult.objects.filter(
                device_id=device_id
            ).delete()
            self.stdout.write(
                f"[anomaly 재실행] 좌표 변경분 반영을 위해 기존 결과 "
                f"{deleted_anomaly}건 삭제 후 재실행"
            )

            saved_anomaly = 0
            for geo_obj in GeoProcessedData.objects.filter(
                device_id=device_id
            ).order_by("timestamp"):
                anomaly_result = run_anomaly_for_latest(geo_obj=geo_obj, minutes=180)
                if anomaly_result.get("anomaly_status") == "saved":
                    saved_anomaly += 1

            self.stdout.write(
                f"[anomaly 재실행] 완료: geo_trip_anomaly_result {saved_anomaly}건 저장"
            )
