from connect import create_test_engine
from sqlalchemy.orm import Session
from model import Singer, Album

engine = create_test_engine()


def load_singer(singer_id):
  with Session(engine) as session:
    singer = session.get(Singer, singer_id)
    print(singer)
    print("Albums:")
    print(singer.albums)


def load_album(album_id):
  with Session(engine) as session:
    album = session.get(Album, album_id)
    print(album)
    print("Tracks:")
    print(album.tracks)
