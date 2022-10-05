#from django.http import HttpResponse

from django.conf import settings
from django.apps import apps

conf = {
    'INSTALLED_APPS': [
        'django.contrib.admin',
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'django.contrib.sessions',
        'django.contrib.messages',
        'django.contrib.staticfiles',
        'data'
    ],
    'DATABASES' : {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'postgres',
            'USER': 'postgres',
            'PASSWORD': 'postgres',
            'HOST': 'localhost',
            'PORT': '5431'
        }
    },
}
settings.configure(**conf)
apps.populate(settings.INSTALLED_APPS)

from data.models import Singer

def get_all_data():
  try:
    object = Singer.objects.all().values()
  except Exception as e:
    print(e)
    object = None
  for x in object:
    print(x)

get_all_data()
