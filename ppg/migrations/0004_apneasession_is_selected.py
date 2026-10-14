from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ppg', '0003_shift_datetimes_to_kst'),
    ]

    operations = [
        migrations.AddField(
            model_name='apneasession',
            name='is_selected',
            field=models.BooleanField(default=False, db_index=True),
        ),
    ]