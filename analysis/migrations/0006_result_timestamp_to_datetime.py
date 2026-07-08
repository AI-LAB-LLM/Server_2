from datetime import datetime, timedelta, timezone as dt_timezone

from django.db import migrations, models

KST = dt_timezone(timedelta(hours=9))


def forwards(apps, schema_editor):
    Result = apps.get_model("analysis", "Result")
    for result in Result.objects.all():
        ms = result.timestamp_ms
        result.timestamp_dt = datetime.fromtimestamp(ms / 1000, tz=KST).replace(tzinfo=None)
        result.save(update_fields=["timestamp_dt"])


def backwards(apps, schema_editor):
    Result = apps.get_model("analysis", "Result")
    for result in Result.objects.all():
        aware = result.timestamp_dt.replace(tzinfo=KST)
        result.timestamp_ms = int(aware.timestamp() * 1000)
        result.save(update_fields=["timestamp_ms"])


class Migration(migrations.Migration):

    dependencies = [
        ("analysis", "0005_shift_datetimes_to_kst"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="result",
            name="analysis_re_device__02e38b_idx",
        ),
        migrations.RenameField(
            model_name="result",
            old_name="timestamp",
            new_name="timestamp_ms",
        ),
        migrations.AddField(
            model_name="result",
            name="timestamp_dt",
            field=models.DateTimeField(null=True),
        ),
        migrations.RunPython(forwards, backwards),
        migrations.RemoveField(
            model_name="result",
            name="timestamp_ms",
        ),
        migrations.RenameField(
            model_name="result",
            old_name="timestamp_dt",
            new_name="timestamp",
        ),
        migrations.AlterField(
            model_name="result",
            name="timestamp",
            field=models.DateTimeField(help_text="분석 결과가 대표하는 기준 시각 (KST)"),
        ),
        migrations.AddIndex(
            model_name="result",
            index=models.Index(fields=["device_id", "timestamp"], name="analysis_re_device__02e38b_idx"),
        ),
    ]
