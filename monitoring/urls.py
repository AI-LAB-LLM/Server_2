from django.urls import path
from . import views

urlpatterns = [
    path("sensor-window/", views.create_sensor_window),

]