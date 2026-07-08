from datetime import timedelta

from django.db import migrations
from django.db.models import F

FIELDS_BY_MODEL = {
    "ApneaSession": ["started_at", "created_at"],
    "ApneaResult": ["processed_at"],
}


def shift(apps, delta):
    for model_name, fields in FIELDS_BY_MODEL.items():
        model = apps.get_model("ppg", model_name)
        model.objects.update(**{name: F(name) + delta for name in fields})


def forwards(apps, schema_editor):
    shift(apps, timedelta(hours=9))


def backwards(apps, schema_editor):
    shift(apps, timedelta(hours=-9))


class Migration(migrations.Migration):

    dependencies = [
        ("ppg", "0002_remove_apneasession_model_config"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
