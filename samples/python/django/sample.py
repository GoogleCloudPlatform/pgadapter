import os
import sys
import datetime
import django
import pytz
from django.db import connection
from django.db import transaction
x = 0

def get_singer(singer_id):
  global x
  x += 1
  return Singer(id=singer_id, first_name='singer', last_name=str(x), full_name= 'singer'+str(x), active=True, created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))

def get_album(album_id, singer=None):
  global x
  x += 1
  return Album(id=album_id, singer=singer, title='album'+str(x), marketing_budget = 200000, release_date = datetime.date.today(), cover_picture= b'hello world',created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))

def get_track(track_id, track_number, album = None):
  global x
  x += 1
  return Track(track_id=track_id, album=album, track_number=track_number, title='track'+str(x), sample_rate=124.543, created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))

def get_venue(venue_id):
  global x
  x += 1
  return Venue(id=venue_id, name='venue'+str(x), description='description'+str(x), created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))

def get_concert(concert_id, venue = None, singer = None):
  global x
  x += 1
  return Concert(id=concert_id, venue=venue, singer=singer, name='concert'+str(x), start_time=datetime.datetime.now(pytz.UTC), end_time=datetime.datetime.now(pytz.UTC)+datetime.timedelta(hours=1), created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))

def create_tables():
  file = open('create_data_model.sql', 'r')
  ddl_statements = file.read()
  with connection.cursor() as cursor:
    cursor.execute(ddl_statements)

def test_adding_data():
  with transaction.atomic():
    singer = get_singer('1')
    singer.save()

    album = get_album('1', singer)
    album.save()

    track = get_track('1', '2', album)
    track.save(force_insert=True)

    venue = get_venue('1')
    venue.save()

    concert = get_concert('1', venue, singer)
    concert.save()


def test_deleting_data():
  with transaction.atomic():
    Track.objects.all().delete()
    Album.objects.all().delete()
    Concert.objects.all().delete()
    Venue.objects.all().delete()
    Singer.objects.all().delete()

def test_foreign_key():
  singer1 = Singer.objects.filter(id='1')[0]
  album1 = Album.objects.filter(id='1')[0]

  #originally album1 belongs to singer1
  try:
    assert album1.singer_id==singer1.id
  except:
    raise Exception("Album1 doesn't belong to singer1")

  singer2 = get_singer('2')
  singer2.save()

  global x
  x += 1
  album2 = singer2.album_set.create(id='2', title='album'+str(x), marketing_budget=250000, cover_picture=b'new world', created_at=datetime.datetime.now(pytz.UTC), updated_at=datetime.datetime.now(pytz.UTC))
  album2.singer_id == singer2.id

  #checking if the album2 is actually saved to the db
  try:
    assert len(Album.objects.filter(id=album2.id)) == 1
  except:
    raise Exception("Album2 not found in the db")

  #assigning album1 to singer2
  singer2.album_set.add(album1)

  #checking if album1 belongs to singer2
  try:
    assert album1.singer_id == singer2.id
  except:
    raise Exception("Couldn't change the parent of album1 fromm singer1 to singer2")

def test_transaction_rollback():
  transaction.set_autocommit(False)

  singer3 = get_singer('3')
  singer3.save()

  transaction.rollback()
  try:
    assert len(Singer.objects.filter(id='3')) == 0
  except:
    raise Exception('Transaction Rollback Unsucessful')
  finally:
    transaction.set_autocommit(True)

if __name__ == "__main__":

  try:
    #setting up django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'setting')
    django.setup()
    print('Django Setup Created')

    #creating the tables if they don't exist
    create_tables()
    print('Tables corresponding to data models created')

    #importing the models
    from sample_app.model import Singer, Album, Track, Concert, Venue

    test_adding_data()
    print('Adding Data Successful')

    test_foreign_key()
    print('Testing Foreign Key Successful')

    test_transaction_rollback()
    print('Testing Transaction Rollback Successful')

    test_deleting_data()
    print('Deleting Data Successful')

  except Exception as e:
    print(e)
    test_deleting_data()
    sys.exit(1)