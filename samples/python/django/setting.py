INSTALLED_APPS = [
    'sample_app'
]
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'PORT': '62502',
        'HOST': 'localhost'
    }
}

USE_TZ = True
