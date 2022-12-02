from connect import create_test_engine
from sqlalchemy.orm import Session
from model import Singer, Album, Track

engine = create_test_engine()


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
    print("Added singer {} with full name {}".format(singer.id, singer.full_name))
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
