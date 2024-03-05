''' Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

import sys
import datetime

import pytz


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
              'OPTIONS': {
                'options': '-c TimeZone=UTC',
              },
              'TIME_ZONE': 'UTC'
          }
      }
  }
  conf['DATABASES']['default']['PORT'] = port
  conf['DATABASES']['default']['HOST'] = host
  conf['USE_TZ'] = True
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

def all_types_insert():
  obj = all_types(col_bigint=6,
                  col_bool=True,
                  col_bytea=b'hello',
                  col_date=datetime.date(1998, 10, 2), col_int=13,
                  col_varchar='some text',
                  col_float4=3.14,
                  col_float8=26.8,
                  col_numeric=95.6,
                  col_timestamptz=datetime.datetime.fromtimestamp(1545730176, pytz.UTC),
                  col_jsonb={'key':'value'})
  obj.save()
  print('Insert Successful')

def all_types_insert_all_null():
  obj = all_types()
  obj.save()
  print('Insert Successful')

def select_all_null():
  result = all_types.objects\
    .filter(col_bigint=None,
            col_bool=None,
            col_bytea=None,
            col_float4=None,
            col_float8=None,
            col_int=None,
            col_numeric=None,
            col_timestamptz=None,
            col_date=None,
            col_varchar=None,
            col_jsonb=None)
  for rows in result.values():
    print(rows)

def select_all_types():
  result = all_types.objects

  for row in result.values():
    for key in row:
      print(key,":",end=' ')
      if key == 'col_bytea':
        print(bytes(row[key]),end=' ')
      else:
        print(row[key],end=' ')
      print(',', end=' ')

def execute(option):
  type = option[0]
  if type == 'all':
    get_all_data()
  elif type == 'insert':
    insert_data(option[1:])
  elif type == 'filter':
    get_filtered_data(option[1:])
  elif type == 'all_types_insert':
    all_types_insert()
  elif type == 'all_types_insert_null':
    all_types_insert_all_null()
  elif type == "select_all_null":
    select_all_null()
  elif type == "select_all_types":
    select_all_types()
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
    from data.models import Singer
    from data.models import all_types
  except Exception as e:
    print(e)
    sys.exit(1)

  try:
    execute(sys.argv[3:])
  except Exception as e:
    print(e)
