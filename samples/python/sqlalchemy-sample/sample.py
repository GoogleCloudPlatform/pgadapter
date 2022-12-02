from connect import create_test_engine
from sqlalchemy.orm import Session
from model import Singer, Album, Track
from random_names import random_first_name, random_last_name, \
  random_album_title, random_release_date, random_marketing_budget, \
  random_cover_picture
from uuid import uuid4

engine = create_test_engine()


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


def print_singers_and_albums():
  with Session(engine) as session:
    for singer in session.query(Singer).order_by("last_name").all():
      print(singer)
      for album in singer.albums:
        print(album)


def create_random_singer(num_albums):
  return Singer(
    id="{}".format(uuid4()),
    first_name=random_first_name(),
    last_name=random_last_name(),
    active=True,
    albums=create_random_albums(num_albums)
  )


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


def load_singer(singer_id):
  with Session(engine) as session:
    singer = session.get(Singer, singer_id)
    print(singer)
    print("Albums:")
    print(singer.albums)


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


def load_album(album_id):
  with Session(engine) as session:
    album = session.get(Album, album_id)
    print(album)
    print("Tracks:")
    print(album.tracks)


def load_track(album_id, track_number):
  with Session(engine) as session:
    track = session.get(Track, [album_id, track_number])
    print(track)
