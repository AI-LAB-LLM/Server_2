from django.urls import path

from . import views

urlpatterns = [
    path("results/", views.create_result),
    # path("results/<int:session_id>/", views.session_results),
]