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
from django.db.models import Avg
from django.db.models import Min
from django.db.models import Max
from django.db.models import Count

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
      },
  }
  conf['DATABASES']['default']['PORT'] = port
  conf['DATABASES']['default']['HOST'] = host
  conf['USE_TZ'] = True
  settings.configure(**conf)
  apps.populate(settings.INSTALLED_APPS)

def execute(option):

  type = option[0]

  if type == 'simple_count':
    print(Singer.objects.count())
  elif type == 'aggregate_avg':
    print(Singer.objects.all().aggregate(Avg('singerid')))
  elif type == 'aggregate_min':
    print(Singer.objects.all().aggregate(Min('singerid')))
  elif type == 'aggregate_max':
    print(Singer.objects.all().aggregate(Max('singerid')))
  elif type == 'aggregate_multiple':
    print(Singer.objects.all().aggregate(Max('singerid'), Min('singerid'), Avg('singerid')))
  elif type == 'annotate_count':
    print(Singer.objects.values('firstname').annotate(Count('firstname')))
  elif type == 'annotate_min':
    print(Singer.objects.values('firstname').annotate(Min('singerid')))
  elif type == 'annotate_max':
    print(Singer.objects.values('firstname').annotate(Max('singerid')))
  elif type == 'annotate_avg':
    print(Singer.objects.values('firstname').annotate(Avg('singerid')))
  elif type == 'annotate_multiple':
    print(Singer.objects.values('singerid').annotate(Avg('singerid'), Count('singerid'), Max('singerid'), Min('singerid')))

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
    sys.exit(1)

  try:
    execute(sys.argv[3:])
  except Exception as e:
    print(e)
