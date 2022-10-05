import sys
from django.core.cache import cache

def flush():
  cache.clear()

def create_django_setup(host, port):
  from django.conf import settings
  from django.apps import apps
  conf = {
      'INSTALLED_APPS': [
          'django.contrib.auth',
          'django.contrib.contenttypes',
          'data'
      ],
      'DATABASES': {
          'default': {
              'ENGINE': 'django.db.backends.postgresql_psycopg2',
              'NAME': 'postgres',
              'USER': 'postgres',
              'PASSWORD': 'postgres',
          }
      },
  }
  conf['DATABASES']['default']['PORT'] = port
  conf['DATABASES']['default']['HOST'] = host
  settings.configure(**conf)
  apps.populate(settings.INSTALLED_APPS)

def get_all_data():
  try:
    result = Singer.objects.all().values()
  except Exception as e:
    print(e)
    result = None
  for rows in result:
    print(rows)

def insert_data(data):
  if len(data) == 0 or len(data) % 3:
    print('Invalid Size of Data')
    return
  i = 0
  while i < len(data):
    singerid,firstname, lastname = data[i], data[i+1], data[i+2]
    singer = Singer(singerid=singerid, firstname=firstname, lastname=lastname)
    singer.save()
    print('Save Successful For', singerid, firstname, lastname)
    i += 3

def get_filtered_data(filters):
  if len(filters) == 0:
    print('No Filter Found')
    return
  function_string = 'Singer.objects'

  for filter in filters:
    function_string += '.filter('+filter+')'
  result = eval(function_string)
  for rows in result.values():
    print(rows)

def execute(option):
  type = option[0]
  if type == 'all':
    get_all_data()
  elif type == 'insert':
    insert_data(option[1:])
  elif type == 'filter':
    get_filtered_data(option[1:])
  else:
    print('Invalid Option Type')

if __name__ == '__main__':
  if len(sys.argv) < 4:
    print('Invalid command line arguments')
    sys.exit()
  host = sys.argv[1]
  port = sys.argv[2]

  try:
    create_django_setup(host, port)
    flush()
    from data.models import Singer
  except Exception as e:
    print(e)
    sys.exit()

  try:
    execute(sys.argv[3:])
  except Exception as e:
    print(e)




