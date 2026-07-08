from datetime import timedelta

from django.db import migrations
from django.db.models import F

FIELDS_BY_MODEL = {
    "GeoData": ["timestamp", "created_at"],
    "GeoProcessedData": ["timestamp", "created_at", "updated_at"],
    "GeoTripAnomalyResult": ["trip_start_time", "trip_end_time", "created_at"],
}


def shift(apps, delta):
    for model_name, fields in FIELDS_BY_MODEL.items():
        model = apps.get_model("geo", model_name)
        model.objects.update(**{name: F(name) + delta for name in fields})


def forwards(apps, schema_editor):
    shift(apps, timedelta(hours=9))


def backwards(apps, schema_editor):
    shift(apps, timedelta(hours=-9))


class Migration(migrations.Migration):

    dependencies = [
        ("geo", "0004_rename_lat_lon_columns"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
