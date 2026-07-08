from datetime import timedelta

from django.db import migrations
from django.db.models import F


def forwards(apps, schema_editor):
    ThreatWindow = apps.get_model("ingest", "ThreatWindow")
    ThreatWindow.objects.update(created_at=F("created_at") + timedelta(hours=9))


def backwards(apps, schema_editor):
    ThreatWindow = apps.get_model("ingest", "ThreatWindow")
    ThreatWindow.objects.update(created_at=F("created_at") + timedelta(hours=-9))


class Migration(migrations.Migration):

    dependencies = [
        ("ingest", "0002_rename_ppg1_threatsample_ppg_green_and_more"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
