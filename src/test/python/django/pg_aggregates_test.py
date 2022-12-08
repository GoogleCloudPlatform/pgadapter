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
from django.contrib.postgres.aggregates import StringAgg
from django.contrib.postgres.aggregates import ArrayAgg

def create_django_setup(host, port):
  from django.conf import settings
  from django.apps import apps
  conf = {
      'INSTALLED_APPS': [
          'django.contrib.auth',
          'django.contrib.contenttypes',
          'django.contrib.postgres',
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

def execute(option):
  if option == 'string_agg':
    print(Singer.objects.values('firstname').annotate(str=StringAgg('firstname',delimiter = '|')).query)
  elif option == 'arr_agg':
    print(Singer.objects.values('singerid').aggregate(arr=ArrayAgg('firstname')))


if __name__ == '__main__':

  if len(sys.argv) < 4:
    print('Invalid command line arguments')
    sys.exit()
  host = sys.argv[1]
  port = sys.argv[2]

  try:
    create_django_setup(host, port)
    from data.models import Singer
  except Exception as e:
    print(e)
    sys.exit()

  try:
    execute(sys.argv[3])
  except Exception as e:
    print(e)


