from datetime import timedelta

from django.db import migrations
from django.db.models import F

FIELDS_BY_MODEL = {
    "Protectee": ["created_at", "updated_at"],
    "MonitoringSession": ["started_at", "ended_at", "last_received_at"],
    "SensorWindow": ["started_at", "ended_at", "created_at"],
}


def shift(apps, delta):
    for model_name, fields in FIELDS_BY_MODEL.items():
        model = apps.get_model("monitoring", model_name)
        model.objects.update(**{name: F(name) + delta for name in fields})


def forwards(apps, schema_editor):
    shift(apps, timedelta(hours=9))


def backwards(apps, schema_editor):
    shift(apps, timedelta(hours=-9))


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0004_monitoringsession_last_received_at_window_count"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
