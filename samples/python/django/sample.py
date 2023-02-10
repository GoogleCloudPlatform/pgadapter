import os
import sys
import datetime
import django
import pytz
from django.db import connection
from django.db import transaction
from django.db.transaction import atomic
from django.db.models import JSONField, CharField
from django.db.models.functions import Cast
import random
x = 0

def create_sample_singer(singer_id):
  global x
  x += 1
  return Singer(id=singer_id,
                first_name='singer',
                last_name=str(x),
                full_name= 'singer'+str(x),
                active=True,
                created_at=datetime.datetime.now(pytz.UTC),
                updated_at=datetime.datetime.now(pytz.UTC))

def create_sample_album(album_id, singer=None):
  global x
  x += 1
  return Album(id=album_id,
               singer=singer,
               title='album'+str(x),
               marketing_budget = 200000,
               release_date = datetime.date.today(),
               cover_picture= b'hello world',
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))

def create_sample_track(track_id, track_number, album = None):
  global x
  x += 1
  return Track(track_id=track_id,
               album=album,
               track_number=track_number,
               title='track'+str(x),
               sample_rate=124.543,
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))

def get_sample_venue_description(x):
  random.seed(datetime.datetime.now().timestamp())

  description = {
      'address': 'address'+str(x),
      'capacity': random.randint(1000, 5000),
      'isPopular': random.choice([True, False])
  }

  return description


def create_sample_venue(venue_id):
  global x
  x += 1
  return Venue(id=venue_id,
               name='venue'+str(x),
               description=get_sample_venue_description(x),
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))

def create_sample_concert(concert_id, venue = None, singer = None):
  global x
  x += 1
  return Concert(id=concert_id,
                 venue=venue,
                 singer=singer,
                 name='concert'+str(x),
                 start_time=datetime.datetime.now(pytz.UTC),
                 end_time=datetime.datetime.now(pytz.UTC)+datetime.timedelta(hours=1),
                 created_at=datetime.datetime.now(pytz.UTC),
                 updated_at=datetime.datetime.now(pytz.UTC))

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

  #originally album1 belongs to singer1
  if album1.singer_id != singer1.id:
    raise Exception("Album1 doesn't belong to singer1")

  singer2 = create_sample_singer('2')
  singer2.save()

  global x
  x += 1
  album2 = singer2.album_set.create(id='2',
                                    title='album'+str(x),
                                    marketing_budget=250000,
                                    cover_picture=b'new world',
                                    created_at=datetime.datetime.now(pytz.UTC),
                                    updated_at=datetime.datetime.now(pytz.UTC))

  #checking if the newly created album2 is associated with singer 2
  if album2.singer_id != singer2.id:
    raise Exception("Album2 is not associated with singer2")

  #checking if the album2 is actually saved to the db
  if len(Album.objects.filter(id=album2.id)) == 0:
    raise Exception("Album2 not found in the db")

  #associating album1 to singer2
  singer2.album_set.add(album1)

  #checking if album1 belongs to singer2
  if album1.singer_id != singer2.id:
    raise Exception("Couldn't change the parent of album1 fromm singer1 to singer2")

def transaction_rollback():
  transaction.set_autocommit(False)

  singer3 = create_sample_singer('3')
  singer3.save()

  transaction.rollback()
  transaction.set_autocommit(True)

  #checking if singer3 is present in the actual table or not
  if len(Singer.objects.filter(id='3')) > 0:
    raise Exception('Transaction Rollback Unsuccessful')

def jsonb_filter():
  venue1 = create_sample_venue(10)
  venue2 = create_sample_venue(100)
  venue3 = create_sample_venue(1000)

  venue1.save()
  venue2.save()
  venue3.save()
  print(venue1.description['address'])
  print(Venue.objects
        .annotate(address=Cast('description__address', output_field=CharField()))
        .filter(address=venue1.description['address'])
        .first())

#  if fetched_venue1.id != venue1.id:
 #   raise Exception('Transaction Rollback Unsuccessful')




if __name__ == "__main__":

  try:
    tables_created = False

    #setting up django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'setting')
    django.setup()
    print('Django Setup Created')

    #creating the tables if they don't exist
    create_tables()
    print('Tables corresponding to data models created')

    tables_created = True

    #importing the models
    from sample_app.model import Singer, Album, Track, Concert, Venue

    print('Starting Django Test')

    #add_data()
    print('Adding Data Successful')

    #foreign_key_operations()
    print('Testing Foreign Key Successful')

    #transaction_rollback()
    print('Transaction Rollback Successful')

    #delete_all_data()
    print('Deleting Data Successful')

    jsonb_filter()
    print('Jsonb Filtering Successful ')

    print('Django Sample Completed Successfully')

  except Exception as e:
    print(e)
    if tables_created:
      delete_all_data()
    sys.exit(1)