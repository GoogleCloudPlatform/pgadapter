import os

INSTALLED_APPS = [
  'sample_app'
]
DATABASES = {
    'default': {
        "ENGINE": "django.db.backends.postgresql",
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'PORT': os.getenv('PGPORT', '5432'),
        'HOST': os.getenv('PGHOST', '127.0.0.1'),
    }
}

USE_TZ = True
