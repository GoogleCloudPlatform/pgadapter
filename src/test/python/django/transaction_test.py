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
from django.db import transaction
import sys

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


def test_commit_transaction():
  transaction.set_autocommit(False)
  singer = Singer(singerid=1, firstname='hello', lastname='world')
  singer2 = Singer(singerid=2, firstname='hello', lastname='python')

  singer.save()
  singer2.save()

  transaction.commit()
  print('Transaction Committed')

def test_rollback_transaction():
  transaction.set_autocommit(False)

  singer = Singer(singerid=1, firstname='hello', lastname='world')
  singer2 = Singer(singerid=2, firstname='hello', lastname='python')

  singer.save()
  singer2.save()

  transaction.rollback()
  print('Transaction Rollbacked')

def test_atomic():
  with transaction.atomic():
    singer = Singer(singerid=1, firstname='hello', lastname='world')
    singer2 = Singer(singerid=2, firstname='hello', lastname='python')

    singer.save()
    singer2.save()
  print('Atomic Successful')

def test_nested_atomic():
  with transaction.atomic(savepoint=False):
    singer2 = Singer(singerid=2, firstname='hello', lastname='python')
    with transaction.atomic(savepoint=False):
      singer = Singer(singerid=1, firstname='hello', lastname='world')
      singer.save()
    singer2.save()
  print('Atomic Successful')

def test_error_during_transaction():
  try:
    transaction.set_autocommit(False)

    singer = Singer(singerid=1, firstname='hello', lastname='world')
    singer2 = Singer(singerid=2, firstname='hello', lastname='python')

    singer.save()
    transaction.commit()
  except Exception:
    try:
      singer2.save()
    except Exception as e:
      print(e)



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
    option = sys.argv[3]

    if option == 'commit':
      test_commit_transaction()
    elif option == 'rollback':
      test_rollback_transaction()
    elif option == 'atomic':
      test_atomic()
    elif option == 'nested_atomic':
      test_nested_atomic()
    elif option == 'error_during_transaction':
      test_error_during_transaction()
    else:
      print('Invalid Option')
  except Exception as e:
    print(e)


