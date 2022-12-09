""" Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

from connect import create_test_engine
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text, func, or_
from model import Singer, Album, Track, Venue, Concert
from random_names import random_first_name, random_last_name, \
  random_album_title, random_release_date, random_marketing_budget, \
  random_cover_picture
from uuid import uuid4
from datetime import datetime, date

# This is the default engine that is connected to PostgreSQL (PGAdapter).
# This engine will by default use read/write transactions.
engine = create_test_engine()

# This engine uses read-only transactions instead of read/write transactions.
# It is recommended to use a read-only transaction instead of a read/write
# transaction for all workloads that only read data, as read-only transactions
# do not take any locks.
read_only_engine = engine.execution_options(postgresql_readonly=True)

# This engine uses auto commit instead of transactions, and will execute all
# read operations with a max staleness of 10 seconds. This will result in more
# efficient read operations, but the data that is returned can have a staleness
# of up to 10 seconds.
stale_read_engine = create_test_engine(
  autocommit=True,
  options="?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'")


def run_sample():
  # Create the data model that is used for this sample.
  create_tables_if_not_exists()
  # Delete any existing data before running the sample.
  delete_all_data()

  # First create some random singers and albums.
  create_random_singers_and_albums()
  print_singers_and_albums()

  # Create a venue and a concert.
  create_venue_and_concert_in_transaction()
  print_concerts()

  # Execute a query to get all albums released before 1980.
  print_albums_released_before_1980()
  # Print a subset of all singers using LIMIT and OFFSET.
  print_singers_with_limit_and_offset()
  # Execute a query to get all albums that start with the same letter as the
  # first name or last name of the singer.
  print_albums_first_character_of_title_equal_to_first_or_last_name()

  # Delete an album. This will automatically also delete all related tracks, as
  # Tracks is interleaved in Albums with the option ON DELETE CASCADE.
  with Session(engine) as session:
    album_id = session.query(Album).first().id
  delete_album(album_id)

  # Read all albums using a connection that *could* return stale data.
  with Session(stale_read_engine) as session:
    album = session.get(Album, album_id)
    if album is None:
      print("No album found using a stale read.")
    else:
      print("Album was found using a stale read, even though it has already been deleted.")

  print()
  print("Finished running sample")


def create_tables_if_not_exists():
  # Cloud Spanner does not support DDL in transactions. We therefore turn on
  # autocommit for this connection.
  with engine.execution_options(
      isolation_level="AUTOCOMMIT").connect() as connection:
    with open("create_data_model.sql") as file:
      print("Reading sample data model from file")
      ddl_script = text(file.read())
      print("Executing table creation script")
      connection.execute(ddl_script)
      print("Finished executing table creation script")


# Create five random singers, each with a number of albums.
def create_random_singers_and_albums():
  with Session(engine) as session:
    session.add_all([
      create_random_singer(5),
      create_random_singer(10),
      create_random_singer(7),
      create_random_singer(3),
      create_random_singer(12),
    ])
    session.commit()
    print("Created 5 singers")


# Print all singers and albums in currently in the database.
def print_singers_and_albums():
  with Session(read_only_engine) as session:
    print()
    for singer in session.query(Singer).order_by("last_name").all():
      print("{} has {} albums:".format(singer.full_name, len(singer.albums)))
      for album in singer.albums:
        print("  '{}'".format(album.title))


# Create a Venue and Concert in one read/write transaction.
def create_venue_and_concert_in_transaction():
  with Session(engine) as session:
    singer = session.query(Singer).first()
    venue = Venue(
      id="{}".format(uuid4()),
      name="Avenue Park",
      description='{'
                  '  "Capacity": 5000,'
                  '  "Location": "New York",'
                  '  "Country": "US"'
                  '}'
    )
    concert = Concert(
      id="{}".format(uuid4()),
      name="Avenue Park Open",
      venue=venue,
      singer=singer,
      start_time=datetime.fromisoformat("2023-02-01T20:00:00-05:00"),
      end_time=datetime.fromisoformat("2023-02-02T02:00:00-05:00"),
    )
    session.add_all([venue, concert])
    session.commit()
    print()
    print("Created Venue and Concert")


# Prints the concerts currently in the database.
def print_concerts():
  with Session(read_only_engine) as session:
    # Query all concerts and join both Singer and Venue, so we can directly
    # access the properties of these as well without having to execute
    # additional queries.
    concerts = session.query(Concert) \
      .options(joinedload(Concert.venue)) \
      .options(joinedload(Concert.singer)) \
      .order_by("start_time") \
      .all()
    print()
    for concert in concerts:
      print("Concert '{}' starting at {} with {} will be held at {}"
            .format(concert.name,
                    concert.start_time,
                    concert.singer.full_name,
                    concert.venue.name))


# Prints all albums with a release date before 1980-01-01.
def print_albums_released_before_1980():
  with Session(read_only_engine) as session:
    print()
    print("Searching for albums released before 1980")
    albums = session \
      .query(Album) \
      .filter(Album.release_date < date.fromisoformat("1980-01-01")) \
      .all()
    for album in albums:
      print(
        "  Album {} was released at {}".format(album.title, album.release_date))


# Uses a limit and offset to select a subset of all singers in the database.
def print_singers_with_limit_and_offset():
  with Session(read_only_engine) as session:
    print()
    print("Printing at most 5 singers ordered by last name")
    singers = session \
      .query(Singer) \
      .order_by(Singer.last_name) \
      .limit(5) \
      .offset(3) \
      .all()
    num_singers = 0
    for singer in singers:
      num_singers = num_singers + 1
      print("  {}. {}".format(num_singers, singer.full_name))
    print("Found {} singers".format(num_singers))


# Searches for all albums that have a title that starts with the same character
# as the first character of either the first name or last name of the singer.
def print_albums_first_character_of_title_equal_to_first_or_last_name():
  print()
  print("Searching for albums that have a title that starts with the same "
        "character as the first or last name of the singer")
  with Session(read_only_engine) as session:
    albums = session \
      .query(Album) \
      .join(Singer) \
      .filter(or_(func.lower(func.substring(Album.title, 1, 1)) ==
                  func.lower(func.substring(Singer.first_name, 1, 1)),
                  func.lower(func.substring(Album.title, 1, 1)) ==
                  func.lower(func.substring(Singer.last_name, 1, 1)))) \
      .all()
    for album in albums:
      print("  '{}' by {}".format(album.title, album.singer.full_name))


# Creates a random singer row with `num_albums` random albums.
def create_random_singer(num_albums):
  return Singer(
    id="{}".format(uuid4()),
    first_name=random_first_name(),
    last_name=random_last_name(),
    active=True,
    albums=create_random_albums(num_albums)
  )


# Creates `num_albums` random album rows.
def create_random_albums(num_albums):
  albums = [None] * num_albums
  for i in range(num_albums):
    albums[i] = Album(
      id="{}".format(uuid4()),
      title=random_album_title(),
      release_date=random_release_date(),
      marketing_budget=random_marketing_budget(),
      cover_picture=random_cover_picture()
    )
  return albums


# Loads and prints the singer with the given id.
def load_singer(singer_id):
  with Session(engine) as session:
    singer = session.get(Singer, singer_id)
    print(singer)
    print("Albums:")
    print(singer.albums)


# Adds a new singer row to the database. Shows how flushing the session will
# automatically return the generated `full_name` column of the Singer.
def add_singer(singer):
  with Session(engine) as session:
    session.add(singer)
    # We flush the session here to show that the generated column full_name is
    # returned after the insert. Otherwise, we could just execute a commit
    # directly.
    session.flush()
    print(
      "Added singer {} with full name {}".format(singer.id, singer.full_name))
    session.commit()


# Updates an existing singer in the database. This will also automatically
# update the full_name of the singer. This is returned by the database and is
# visible in the properties of the singer.
def update_singer(singer_id, first_name, last_name):
  with Session(engine) as session:
    singer = session.get(Singer, singer_id)
    singer.first_name = first_name
    singer.last_name = last_name
    # We flush the session here to show that the generated column full_name is
    # returned after the update. Otherwise, we could just execute a commit
    # directly.
    session.flush()
    print("Updated singer {} with full name {}"
          .format(singer.id, singer.full_name))
    session.commit()


# Loads the given album from the database and prints its properties, including
# the tracks related to this album. The table `tracks` is interleaved in
# `albums`. This is handled as a normal relationship in SQLAlchemy.
def load_album(album_id):
  with Session(engine) as session:
    album = session.get(Album, album_id)
    print(album)
    print("Tracks:")
    print(album.tracks)


# Loads a single track from the database and prints its properties. Track has a
# composite primary key, as it must include both the primary key column(s) of
# the parent table, as well as its own primary key column(s).
def load_track(album_id, track_number):
  with Session(engine) as session:
    # The "tracks" table has a composite primary key, as it is an interleaved
    # child table of "albums".
    track = session.get(Track, [album_id, track_number])
    print(track)


# Deletes all current sample data.
def delete_all_data():
  with Session(engine) as session:
    session.query(Concert).delete()
    session.query(Venue).delete()
    session.query(Album).delete()
    session.query(Singer).delete()
    session.commit()


# Deletes an album from the database. This will also delete all related tracks.
# The deletion of the tracks is done by the database, and not by SQLAlchemy, as
# passive_deletes=True has been set on the relationship.
def delete_album(album_id):
  with Session(engine) as session:
    album = session.get(Album, album_id)
    session.delete(album)
    session.commit()
    print()
    print("Deleted album with id {}".format(album_id))
