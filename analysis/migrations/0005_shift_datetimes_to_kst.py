from datetime import timedelta

from django.db import migrations
from django.db.models import F

FIELDS = ["created_at"]


def shift(model, delta):
    model.objects.update(**{name: F(name) + delta for name in FIELDS})


def forwards(apps, schema_editor):
    Result = apps.get_model("analysis", "Result")
    shift(Result, timedelta(hours=9))


def backwards(apps, schema_editor):
    Result = apps.get_model("analysis", "Result")
    shift(Result, timedelta(hours=-9))


class Migration(migrations.Migration):

    dependencies = [
        ("analysis", "0004_alter_result_id"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
