INSTALLED_APPS = [
    'sample_app'
]
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'my-database',
        'PORT': '5432',
        'HOST': 'localhost'
    }
}

USE_TZ = True
