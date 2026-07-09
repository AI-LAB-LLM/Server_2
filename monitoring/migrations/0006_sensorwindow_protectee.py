import django.db.models.deletion
from django.db import migrations, models


def backfill_protectee(apps, schema_editor):
    SensorWindow = apps.get_model("monitoring", "SensorWindow")
    MonitoringSession = apps.get_model("monitoring", "MonitoringSession")
    SensorWindow.objects.filter(protectee_id__isnull=True).update(
        protectee_id=models.Subquery(
            MonitoringSession.objects.filter(
                pk=models.OuterRef("session_id")
            ).values("protectee_id")[:1]
        )
    )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0005_shift_datetimes_to_kst"),
    ]

    operations = [
        migrations.AddField(
            model_name="sensorwindow",
            name="protectee",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="sensor_windows",
                to="monitoring.protectee",
                help_text="session.protectee와 동일. 조회 편의를 위한 매핑 컬럼",
            ),
        ),
        migrations.RunPython(backfill_protectee, noop_reverse),
        migrations.AlterField(
            model_name="sensorwindow",
            name="protectee",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="sensor_windows",
                to="monitoring.protectee",
                help_text="session.protectee와 동일. 조회 편의를 위한 매핑 컬럼",
            ),
        ),
        migrations.AddIndex(
            model_name="sensorwindow",
            index=models.Index(fields=["protectee"], name="monitoring__protect_a6ddcf_idx"),
        ),
        migrations.RemoveField(
            model_name="sensorwindow",
            name="sample_rate_hz",
        ),
        migrations.RemoveField(
            model_name="sensorwindow",
            name="duration_sec",
        ),
    ]
