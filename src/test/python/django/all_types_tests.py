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
          }
      },
  }
  conf['DATABASES']['default']['PORT'] = port
  conf['DATABASES']['default']['HOST'] = host
  conf['USE_TZ'] = True
  settings.configure(**conf)
  apps.populate(settings.INSTALLED_APPS)

def get_all_data():
  for p in all_types.objects.raw('Select * from all_types where col_varchar is NULL'):
    print('----->',p.col_bigint,'----->',p.col_varchar,'----->',sep = '')



def insert_data(data):
  obj2 = all_types(col_bigint=69)
  obj = all_types(col_bigint=6, col_bool=True, col_bytea=b'hello', col_date=datetime.date(1998, 10,1), col_int=13, col_varchar='gaurav jha', col_float8=26.8, col_numeric=95.6, col_timestamptz=datetime.datetime.now(tz=pytz.UTC))
  obj2.save()


def get_filtered_data(filters):
  if len(filters) == 0:
    print('No Filter Found')
    return
  function_string = 'all_types.objects'

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
    from data.models import all_types
  except Exception as e:
    print(e)
    sys.exit()

  try:
    execute(sys.argv[3:])
  except Exception as e:
    print(e)
