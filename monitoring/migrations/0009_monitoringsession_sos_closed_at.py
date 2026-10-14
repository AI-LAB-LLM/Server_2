from django.db import migrations, models
from django.utils import timezone


def close_existing_sos(apps, schema_editor):
    # 필드 도입 이전 SOS는 종결 버튼이 없었으므로, 대시보드에 과거 SOS가 뜨지 않도록 일괄 종결 처리
    MonitoringSession = apps.get_model("monitoring", "MonitoringSession")
    MonitoringSession.objects.filter(
        sos_detected_at__isnull=False,
        sos_closed_at__isnull=True,
    ).update(sos_closed_at=timezone.now())


class Migration(migrations.Migration):

    dependencies = [
        ('monitoring', '0008_monitoringsession_threat_cleared_at'),
    ]

    operations = [
        migrations.AddField(
            model_name='monitoringsession',
            name='sos_closed_at',
            field=models.DateTimeField(blank=True, help_text='대시보드에서 SOS 사건을 종결한 시각. null이면 미종결(SOS 표시 유지)', null=True),
        ),
        migrations.RunPython(close_existing_sos, migrations.RunPython.noop),
    ]
