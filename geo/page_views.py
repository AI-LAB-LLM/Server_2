from django.conf import settings
from django.shortcuts import render
from django.views import View


class GeoDeviceMapPageView(View):
    def get(self, request, device_id):
        return render(
            request,
            "geo/device_map.html",
            {
                "device_id": device_id,
                "KAKAO_JS_KEY": settings.KAKAO_JS_KEY,
            },
        )