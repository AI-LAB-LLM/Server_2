import logging
from django.apps import AppConfig
from django.db.models.signals import post_save

logger = logging.getLogger(__name__)


class AnalysisConfig(AppConfig):
    name = "analysis"

    def ready(self):
        try:
            from .models import Result
            from .signals import handle_result_created

            post_save.connect(handle_result_created, sender=Result)
            logger.info("[AnalysisConfig] Result signal connected")
        except Exception as e:
            logger.error(f"[AnalysisConfig] signal connect failed: {e}")
