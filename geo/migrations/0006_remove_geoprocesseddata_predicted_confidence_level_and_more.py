from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('geo', '0005_shift_datetimes_to_kst'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='geoprocesseddata',
            name='predicted_confidence_level',
        ),
        migrations.RemoveField(
            model_name='geoprocesseddata',
            name='predicted_latitude',
        ),
        migrations.RemoveField(
            model_name='geoprocesseddata',
            name='predicted_longitude',
        ),
        migrations.RemoveField(
            model_name='geoprocesseddata',
            name='predicted_uncertainty_m',
        ),
    ]
