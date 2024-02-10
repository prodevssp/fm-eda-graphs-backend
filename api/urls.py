from django.urls import path
from . import views

urlpatterns = [
    path("upload/", views.upload),
    path("calc/", views.calc),
    path("features/", views.get_features),
]
