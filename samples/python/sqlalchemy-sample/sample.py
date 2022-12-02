from connect import create_test_engine
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from model import Singer, Album, Track, Venue, Concert
from random_names import random_first_name, random_last_name, \
  random_album_title, random_release_date, random_marketing_budget, \
  random_cover_picture
from uuid import uuid4
from datetime import datetime, date

engine = create_test_engine()


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
  with Session(engine) as session:
    for singer in session.query(Singer).order_by("last_name").all():
      print(singer)
      for album in singer.albums:
        print(album)


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
    print("Created Venue and Concert")


# Prints the concerts currently in the database.
def print_concerts():
  with Session(engine) as session:
    # Query all concerts and join both Singer and Venue, so we can directly
    # access the properties of these as well without having to execute
    # additional queries.
    concerts = session.query(Concert) \
      .options(joinedload(Concert.venue)) \
      .options(joinedload(Concert.singer)) \
      .order_by("start_time") \
      .all()
    for concert in concerts:
      print("Concert '{}' starting at {} with {} will be held at {}"
            .format(concert.name,
                    concert.start_time,
                    concert.singer.full_name,
                    concert.venue.name))


def print_albums_released_before_1980():
  with Session(engine) as session:
    print("Searching for albums released before 1980")
    albums = session\
      .query(Album) \
      .filter(Album.release_date < date.fromisoformat("1980-01-01")) \
      .all()
    for album in albums:
      print("Album {} was released at {}".format(album.title, album.release_date))


def print_singers_with_limit_and_offset():
  with Session(engine) as session:
    print("Printing all singers ordered by last name")
    singers = session \
      .query(Singer) \
      .order_by(Singer.last_name) \
      .limit(5) \
      .offset(3) \
      .all()
    num_singers = 0
    for singer in singers:
      print(singer)
      num_singers = num_singers + 1
    print("Found {} singers".format(num_singers))


def print_albums_first_character_of_title_equal_to_first_or_last_name():
  print("Searching for albums that have a title that starts with the same "
        "character as the first or last name of the singer")
  with Session(engine) as session:
    albums = session \
      .query(Album) \
      .join(Singer) \
      .filter(or_(func.lower(func.substring(Album.title, 1, 1)) ==
              func.lower(func.substring(Singer.first_name, 1, 1)),
              func.lower(func.substring(Album.title, 1, 1)) ==
              func.lower(func.substring(Singer.last_name, 1, 1)))) \
      .all()
    for album in albums:
      print(album)


# func PrintAlbumsFirstCharTitleAndFirstOrLastNameEqual(db *gorm.DB) error {
#   fmt.Println("Searching for albums that have a title that starts with the same character as the first or last name of the singer")
# var albums []*Album
#     // Join the Singer association to use it in the Where clause.
#                                                           // Note that `gorm` will use "Singer" (including quotes) as the alias for the singers table.
# // That means that all references to "Singer" in the query must be quoted, as PostgreSQL treats
# // the alias as case-sensitive.
# if err := db.Joins("Singer").Where(
# `lower(substring(albums.title, 1, 1)) = lower(substring("Singer".first_name, 1, 1))` +
# `or lower(substring(albums.title, 1, 1)) = lower(substring("Singer".last_name, 1, 1))`,
# ).Order(`"Singer".last_name, "albums".release_date asc`).Find(&albums).Error; err != nil {
# fmt.Printf("Failed to load albums: %v\n", err)
# return err
# }
# if len(albums) == 0 {
# fmt.Println("No albums found that match the criteria")
# } else {
# for _, album := range albums {
# fmt.Printf("Album %q was released by %v\n", album.Title, album.Singer.FullName)
# }
# }
# fmt.Print("\n\n")
# return nil
# }



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
