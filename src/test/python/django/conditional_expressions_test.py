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
from django.db.models import F, Q, When, Case, Value, Count


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

def execute(option):

  if option == 'update':
    update_count = Singer.objects.update(firstname=Case(
        When(firstname='hello', then=Value('h')),
        When(firstname='world', then=Value('w')),
        default=Value('n')
    ))
    print(update_count)
  elif option == 'aggregation':
    aggregate = Singer.objects.aggregate(
        hello_count=Count('pk', filter=Q(firstname='hello')),
        world_count=Count('pk', filter=Q(firstname='world'))
    )

    print(aggregate)


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
