from django.db import migrations


REORDER_SQL = """
CREATE TABLE "analysis_result_new" (
    "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    "device_id" varchar(100) NOT NULL,
    "mode" varchar(20) NOT NULL,
    "event_type" varchar(10) NOT NULL,
    "timestamp" bigint NOT NULL,
    "risk_level" smallint unsigned NULL CHECK ("risk_level" >= 0),
    "risk_detected" bool NULL,
    "probability" real NULL,
    "created_at" datetime NOT NULL
);

INSERT INTO "analysis_result_new" (
    "id", "device_id", "mode", "event_type", "timestamp",
    "risk_level", "risk_detected", "probability", "created_at"
)
SELECT
    "id", "device_id", "mode", "event_type", "timestamp",
    "risk_level", "risk_detected", "probability", "created_at"
FROM "analysis_result";

DROP TABLE "analysis_result";

ALTER TABLE "analysis_result_new" RENAME TO "analysis_result";

CREATE INDEX "analysis_result_device_id_ef5cdab6" ON "analysis_result" ("device_id");
CREATE INDEX "analysis_re_device__02e38b_idx" ON "analysis_result" ("device_id", "timestamp");
CREATE INDEX "analysis_re_mode_5a3001_idx" ON "analysis_result" ("mode");
CREATE INDEX "analysis_re_event_t_7d3e32_idx" ON "analysis_result" ("event_type");
CREATE INDEX "analysis_re_risk_de_df6f5e_idx" ON "analysis_result" ("risk_detected");
"""


class Migration(migrations.Migration):
    dependencies = [
        ("analysis", "0002_alter_result_event_type_alter_result_mode_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql=REORDER_SQL,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
