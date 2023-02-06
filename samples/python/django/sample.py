import os
import sys
import datetime
import django
import pytz
from django.db import connection
from django.db import transaction
from django.db.transaction import atomic

x = 0


def create_sample_singer(singer_id):
  global x
  x += 1
  return Singer(id=singer_id,
                first_name='singer',
                last_name=str(x),
                full_name='singer' + str(x),
                active=True,
                created_at=datetime.datetime.now(pytz.UTC),
                updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_album(album_id, singer=None):
  global x
  x += 1
  return Album(id=album_id,
               singer=singer,
               title='album' + str(x),
               marketing_budget=200000,
               release_date=datetime.date.today(),
               cover_picture=b'hello world',
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_track(track_id, track_number, album=None):
  global x
  x += 1
  return Track(track_id=track_id,
               album=album,
               track_number=track_number,
               title='track' + str(x),
               sample_rate=124.543,
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_venue(venue_id):
  global x
  x += 1
  return Venue(id=venue_id,
               name='venue' + str(x),
               description='description' + str(x),
               created_at=datetime.datetime.now(pytz.UTC),
               updated_at=datetime.datetime.now(pytz.UTC))


def create_sample_concert(concert_id, venue=None, singer=None):
  global x
  x += 1
  return Concert(id=concert_id,
                 venue=venue,
                 singer=singer,
                 name='concert' + str(x),
                 start_time=datetime.datetime.now(pytz.UTC),
                 end_time=datetime.datetime.now(pytz.UTC) + datetime.timedelta(
                   hours=1),
                 created_at=datetime.datetime.now(pytz.UTC),
                 updated_at=datetime.datetime.now(pytz.UTC))


def create_tables():
  file = open('create_data_model.sql', 'r')
  ddl_statements = file.read()
  # print(ddl_statements)
  with connection.cursor() as cursor:
    cursor.execute(ddl_statements)


@atomic(savepoint=False)
def add_data():
  try:
    singer = create_sample_singer('1')
    singer.save()

    singer_object = Singer.objects.get(id=singer.id)
    if singer_object.first_name != singer.first_name or singer_object.last_name != singer.last_name:
      raise Exception('Saving Singer Data Failed')

    album = create_sample_album('1', singer)
    album.save()

    album_object = Album.objects.get(id=album.id)
    if album_object.title != album.title:
      raise Exception('Saving Album Data Failed')

    track = create_sample_track('1', '2', album)
    track.save(force_insert=True)
    track_object = Track.objects.get(track_number=track.track_number,
                                     album=track.album)
    if track_object.title != track.title:
      raise Exception('Saving Track Data Failed')

    venue = create_sample_venue('1')
    venue.save()
    venue_object = Venue.objects.get(id=venue.id)
    if venue_object.name != venue.name:
      raise Exception('Saving Venue Data Failed')

    concert = create_sample_concert('1', venue, singer)
    concert.save()
    concert_object = Concert.objects.get(id=concert.id)
    if concert_object.name != concert.name:
      raise Exception('Saving Concert Data Failed')
  except Exception as e:
    raise Exception(e)


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

  # originally album1 belongs to singer1
  if album1.singer_id != singer1.id:
    raise Exception("Album1 doesn't belong to singer1")

  singer2 = create_sample_singer('2')
  singer2.save()

  global x
  x += 1
  album2 = singer2.album_set.create(id='2',
                                    title='album' + str(x),
                                    marketing_budget=250000,
                                    cover_picture=b'new world',
                                    created_at=datetime.datetime.now(pytz.UTC),
                                    updated_at=datetime.datetime.now(pytz.UTC))

  # checking if the newly created album2 is associated with singer 2
  if album2.singer_id != singer2.id:
    raise Exception("Album2 is not associated with singer2")

  # checking if the album2 is actually saved to the db
  if len(Album.objects.filter(id=album2.id)) == 0:
    raise Exception("Album2 not found in the db")

  # associating album1 to singer2
  singer2.album_set.add(album1)

  # checking if album1 belongs to singer2
  if album1.singer_id != singer2.id:
    raise Exception(
      "Couldn't change the parent of album1 fromm singer1 to singer2")


def transaction_rollback():
  transaction.set_autocommit(False)

  singer3 = create_sample_singer('3')
  singer3.save()

  transaction.rollback()
  transaction.set_autocommit(True)

  # checking if singer3 is present in the actual table or not
  if len(Singer.objects.filter(id='3')) > 0:
    raise Exception('Transaction Rollback Unsuccessful')


def interleaved_table_update():
  Track.objects.filter(track_number='2', album_id='1').update(sample_rate=0.25)
  updated_track = Track.objects.get(track_number='2', album_id='1')
  if updated_track.sample_rate != 0.25:
    raise Exception('Update on Interleaved Table Failed')


def fetch_data():
  singer1 = create_sample_singer('2')
  singer2 = create_sample_singer('3')

  singer1.save()
  singer2.save()

  fetched_singer1 = Singer.objects.get(full_name=singer1.full_name)
  fetched_singer2 = Singer.objects.get(full_name=singer2.full_name)

  if fetched_singer1.id != singer1.id or fetched_singer2.id != singer2.id:
    raise Exception('Fetching Singer Data Failed')

  album1 = create_sample_album('2', singer1)
  album2 = create_sample_album('3', singer2)

  album1.save()
  album2.save()

  fetched_album1 = Album.objects.get(title=album1.title)
  fetched_album2 = Album.objects.get(title=album2.title)

  if fetched_album1.id != album1.id or fetched_album2.id != album2.id:
    raise Exception('Fetching Album Data Failed')

  track1 = create_sample_track('2', '5', album1)
  track2 = create_sample_track('3', '6', album2)

  track1.save(force_insert=True)
  track2.save(force_insert=True)

  fetched_track1 = Track.objects.get(title=track1.title)
  fetched_track2 = Track.objects.get(title=track2.title)

  if fetched_track1.track_id != track1.track_id or fetched_track2.track_id != track2.track_id:
    raise Exception('Fetching Track Data Failed')

  venue1 = create_sample_venue('2')
  venue2 = create_sample_venue('3')

  venue1.save()
  venue2.save()

  fetched_venue1 = Venue.objects.get(name=venue1.name)
  fetched_venue2 = Venue.objects.get(name=venue2.name)

  if fetched_venue1.id != venue1.id or fetched_venue2.id != venue2.id:
    raise Exception('Fetching Venue Data Failed')

  concert1 = create_sample_concert('2', venue1, singer2)
  concert2 = create_sample_concert('3', venue2, singer1)

  concert1.save()
  concert2.save()

  fetched_concert1 = Concert.objects.get(name=concert1.name)
  fetched_concert2 = Concert.objects.get(name=concert2.name)

  if fetched_concert1.id != concert1.id or fetched_concert2.id != concert2.id:
    raise Exception("Fetching Concert Data Failed")




if __name__ == "__main__":

  try:
    tables_created = False

    # setting up django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'setting')
    django.setup()
    print('Django Setup Created')

    # creating the tables if they don't exist
    create_tables()
    print('Tables corresponding to data models created')

    tables_created = True

    # importing the models
    from sample_app.model import Singer, Album, Track, Concert, Venue

    print('Starting Django Test')

    add_data()
    print('Adding Data Successful')

    foreign_key_operations()
    print('Testing Foreign Key Successful')

    transaction_rollback()
    print('Transaction Rollback Successful')

    interleaved_table_update()
    print('Interleaved Table Update Successful')

    fetch_data()
    print('Fetching Data Successful')

    delete_all_data()
    print('Deleting Data Successful')

    print('Django Sample Completed Successfully')

  except Exception as e:
    print(e)
    if tables_created:
      delete_all_data()
    sys.exit(1)
