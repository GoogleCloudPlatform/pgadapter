from django.http import HttpResponse
from .models import Singer

def index(request):
  try:
    object = Singer.objects.get_all()
  except:
    object = None
  print(object)
  return HttpResponse("hello")
