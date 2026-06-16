import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0002_alter_monitoringsession_mode"),
        ("geo", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="GeoProcessedData",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("device_id", models.CharField(max_length=100)),
                ("timestamp", models.DateTimeField()),
                ("raw_latitude", models.FloatField(blank=True, null=True)),
                ("raw_longitude", models.FloatField(blank=True, null=True)),
                ("latitude", models.FloatField(blank=True, null=True)),
                ("longitude", models.FloatField(blank=True, null=True)),
                ("pos_success", models.BooleanField(default=True)),
                ("gps_quality", models.CharField(blank=True, max_length=50, null=True)),
                (
                    "gps_filter_decision",
                    models.CharField(blank=True, max_length=100, null=True),
                ),
                ("use_raw_for_gpr", models.BooleanField(blank=True, null=True)),
                (
                    "interp_method",
                    models.CharField(blank=True, max_length=100, null=True),
                ),
                ("predicted_latitude", models.FloatField(blank=True, null=True)),
                ("predicted_longitude", models.FloatField(blank=True, null=True)),
                ("predicted_uncertainty_m", models.FloatField(blank=True, null=True)),
                (
                    "predicted_confidence_level",
                    models.CharField(blank=True, max_length=50, null=True),
                ),
                (
                    "state_primary",
                    models.CharField(blank=True, max_length=50, null=True),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "protectee",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="geo_processed_data",
                        to="monitoring.protectee",
                    ),
                ),
            ],
            options={
                "db_table": "geo_processed_data",
                "ordering": ["timestamp"],
            },
        ),
        migrations.CreateModel(
            name="GeoTripAnomalyResult",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("device_id", models.CharField(max_length=100)),
                ("trip_start_time", models.DateTimeField()),
                ("trip_end_time", models.DateTimeField()),
                (
                    "final_route_label",
                    models.CharField(
                        choices=[
                            ("known_normal", "기존 정상 경로"),
                            ("unseen_path_same_od", "같은 OD의 새로운 경로"),
                            ("anomaly", "이상 경로"),
                        ],
                        max_length=50,
                    ),
                ),
                ("od_key", models.CharField(blank=True, max_length=100, null=True)),
                ("dtw_score", models.FloatField(blank=True, null=True)),
                ("threshold", models.FloatField(blank=True, null=True)),
                ("message", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "protectee",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="geo_trip_anomaly_results",
                        to="monitoring.protectee",
                    ),
                ),
            ],
            options={
                "db_table": "geo_trip_anomaly_result",
                "ordering": ["trip_start_time"],
            },
        ),
        migrations.AddIndex(
            model_name="geoprocesseddata",
            index=models.Index(
                fields=["device_id", "timestamp"],
                name="geo_process_device__6b366b_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="geoprocesseddata",
            index=models.Index(
                fields=["protectee", "timestamp"],
                name="geo_process_protect_2e5a10_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="geoprocesseddata",
            index=models.Index(
                fields=["state_primary"], name="geo_process_state_p_7d1cde_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="geoprocesseddata",
            index=models.Index(
                fields=["pos_success"], name="geo_process_pos_suc_6fdef2_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="geotripanomalyresult",
            index=models.Index(
                fields=["device_id", "trip_start_time"],
                name="geo_trip_an_device__e847f7_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="geotripanomalyresult",
            index=models.Index(
                fields=["protectee", "trip_start_time"],
                name="geo_trip_an_protect_8892b4_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="geotripanomalyresult",
            index=models.Index(
                fields=["final_route_label"],
                name="geo_trip_an_final_r_ccfd78_idx",
            ),
        ),
    ]
