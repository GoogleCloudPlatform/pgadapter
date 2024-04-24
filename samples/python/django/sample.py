# Copyright 2024 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import datetime
import django
import pytz
import random
import string
import sys
from django.db import connection
from django.db import transaction
from django.db.transaction import atomic
from django.db.models import IntegerField, CharField, BooleanField
from django.db.models.functions import Cast
from pgadapter import start_pgadapter


def create_sample_singer(singer_id):
  return Singer(id=singer_id,
                first_name='singer',
                last_name=random_string(10),
                full_name=random_string(20),
                active=True,
                created_at=datetime.datetime.now(pytz.UTC),
                updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_album(album_id, singer=None):
  return Album(id=album_id,
               singer=singer,
               title=random_string(10),
               marketing_budget=200000,
               release_date=datetime.date.today(),
               cover_picture=b'hello world',
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_track(track_id, track_number, album = None):
  return Track(track_id=track_id,
               album=album,
               track_number=track_number,
               title=random_string(15),
               sample_rate=124.543,
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def get_sample_venue_description():
  random.seed(datetime.datetime.now().timestamp())

  description = {
      'address': random_string(20),
      'capacity': random.randint(1000, 50000),
      'isPopular': random.choice([True, False])
  }

  return description


def create_sample_venue(venue_id):
  return Venue(id=venue_id,
               name=random_string(10),
               description=get_sample_venue_description(),
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_concert(concert_id, venue = None, singer = None):
  return Concert(id=concert_id,
                 venue=venue,
                 singer=singer,
                 name=random_string(20),
                 start_time=datetime.datetime.now(pytz.UTC),
                 end_time=datetime.datetime.now(pytz.UTC) + datetime.timedelta(hours=1),
                 created_at=datetime.datetime.now(pytz.UTC),
                 updated_at=datetime.datetime.now(pytz.UTC))


def random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))


def create_tables():
  file = open('create_data_model.sql', 'r')
  ddl_statements = file.read()
  print(ddl_statements)
  with connection.cursor() as cursor:
    cursor.execute(ddl_statements)


@atomic(savepoint=False)
def add_data():
  singer = create_sample_singer('1')
  singer.save()

  album = create_sample_album('1', singer)
  album.save()

  track = create_sample_track('1', '2', album)
  track.save(force_insert=True)

  venue = create_sample_venue('1')
  venue.save()

  concert = create_sample_concert('1', venue, singer)
  concert.save()


@atomic(savepoint=False)
def delete_all_data():
  Track.objects.all().delete()
  Album.objects.all().delete()
  Concert.objects.all().delete()
  Venue.objects.all().delete()
  Singer.objects.all().delete()


def foreign_key_operations():
  singer1 = Singer.objects.filter(id='1')[0]
  album1 = Album.objects.filter(id='1')[0]

  # Originally album1 belongs to singer1
  if album1.singer_id != singer1.id:
    raise Exception("Album1 doesn't belong to singer1")

  singer2 = create_sample_singer('2')
  singer2.save()

  album2 = singer2.album_set.create(id='2',
                                    title=random_string(20),
                                    marketing_budget=250000,
                                    cover_picture=b'new world',
                                    created_at=datetime.datetime.now(pytz.UTC),
                                    updated_at=datetime.datetime.now(pytz.UTC))

  # Checking if the newly created album2 is associated with singer 2
  if album2.singer_id != singer2.id:
    raise Exception("Album2 is not associated with singer2")

  # Checking if the album2 is actually saved to the db
  if len(Album.objects.filter(id=album2.id)) == 0:
    raise Exception("Album2 not found in the db")

  # Associating album1 to singer2
  singer2.album_set.add(album1)

  # Checking if album1 belongs to singer2
  if album1.singer_id != singer2.id:
    raise Exception("Couldn't change the parent of "
                    "album1 fromm singer1 to singer2")


def transaction_rollback():
  transaction.set_autocommit(False)

  singer3 = create_sample_singer('3')
  singer3.save()

  transaction.rollback()
  transaction.set_autocommit(True)

  # Checking if singer3 is present in the actual table or not
  if len(Singer.objects.filter(id='3')) > 0:
    raise Exception('Transaction Rollback Unsuccessful')


def jsonb_filter():
  venue1 = create_sample_venue('10')
  venue2 = create_sample_venue('100')
  venue3 = create_sample_venue('1000')

  venue1.save()
  venue2.save()
  venue3.save()

  # In order to query inside the fields of a jsonb column,
  # we first need to use annotate to cast the respective jsonb
  # field to the relevant data type.
  # In this example, the 'address' field is cast to CharField
  # and then a filter is applied to this field.
  # Make sure to enclose the filter value in double quotes("") for string
  # values.

  fetched_venue1 = Venue.objects.annotate(
      address=Cast('description__address',
                   output_field=CharField())).filter(
      address='"'+venue1.description['address']+'"').first()

  if fetched_venue1.id != venue1.id:
    raise Exception('No Venue found with address '
                    + venue1.description['address'])

  fetched_venue2 = Venue.objects.annotate(
      capacity=Cast('description__capacity',
                    output_field=IntegerField())).filter(
      capacity=venue2.description['capacity']).first()

  if fetched_venue2.id != venue2.id:
    raise Exception('No Venue found with capacity '
                    + venue1.description['capacity'])

  fetched_venues3 = Venue.objects.annotate(
      isPopular=Cast('description__isPopular',
                     output_field=BooleanField())).filter(
      isPopular=venue3.description['isPopular']).only('id')
  if venue3 not in fetched_venues3:
    raise Exception('No Venue found with popularity '
                    + venue1.description['isPopular'])


if __name__ == "__main__":
  # Start PGAdapter in an embedded container.
  container, port = start_pgadapter("emulator-project",
                                    "test-instance",
                                    True,
                                    None)
  os.environ.setdefault("PGHOST", "localhost")
  os.environ.setdefault("PGPORT", str(port))

  tables_created = False
  try:

    # Setting up django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'setting')
    django.setup()
    print('Django Setup Created')

    # Creating the tables if they don't exist
    create_tables()
    print('Tables corresponding to data models created')

    tables_created = True

    # Importing the models
    from sample_app.model import Singer, Album, Track, Concert, Venue

    print('Starting Django Test')

    add_data()
    print('Adding Data Successful')

    foreign_key_operations()
    print('Testing Foreign Key Successful')

    transaction_rollback()
    print('Transaction Rollback Successful')

    jsonb_filter()
    print('Jsonb Filtering Successful ')

    delete_all_data()
    print('Deleting Data Successful')

    print('Django Sample Completed Successfully')

  except Exception as e:
    print(e)
    if tables_created:
      delete_all_data()
    sys.exit(1)
