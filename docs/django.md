# PGAdapter - django Connection Options

PGAdapter has Pilot Support for [django](https://www.djangoproject.com/) version v4.1.1 and higher.

## Limitations
Pilot Support means that it is possible to use `django` with Cloud Spanner PostgreSQL databases, but
with limitations. This means that porting an existing application from PostgreSQL to Cloud Spanner
will probably require code changes. See [Limitations](../samples/python/django/README.md#limitations)
in the `django` sample directory for a full list of limitations.

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

Then connect to PGAdapter and use `django` by including the following in your `setting.py` file:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'PORT': '5432',
        'HOST': 'localhost'
    }
}
```

## Full Sample and Limitations
[This directory](../samples/python/django) contains a full sample of how to work with `django` with
Cloud Spanner and PGAdapter. The sample readme file also lists the [current limitations](../samples/python/django)
when working with `django`.
