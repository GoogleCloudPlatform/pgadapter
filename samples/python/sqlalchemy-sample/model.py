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

from sqlalchemy import Column, Integer, String, Boolean, LargeBinary, Float, \
  Numeric, DateTime, Date, FetchedValue, ForeignKey, ColumnDefault
from sqlalchemy.orm import registry, relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import timezone, datetime

mapper_registry = registry()
Base = mapper_registry.generate_base()


"""
 BaseMixin contains properties that are common to all models in this sample. All
 models use a string column that contains a client-side generated UUID as the
 primary key.
 The created_at and updated_at properties are automatically filled with the
 current client system time when a model is created or updated. 
"""


def format_timestamp(timestamp: datetime) -> str:
  return timestamp.astimezone(timezone.utc).isoformat() if timestamp else None


class BaseMixin(object):
  __prepare_statements__ = None

  id = Column(String, primary_key=True)
  version_id = Column(Integer, nullable=False)
  created_at = Column(DateTime(timezone=True),
                      # We need to explicitly format the timestamps with a
                      # timezone to ensure that SQLAlchemy uses a
                      # timestamptz instead of just timestamp.
                      ColumnDefault(datetime.utcnow().astimezone(timezone.utc)))
  updated_at = Column(DateTime(timezone=True),
                      ColumnDefault(
                        datetime.utcnow().astimezone(timezone.utc),
                        for_update=True))
  __mapper_args__ = {"version_id_col": version_id}


class Singer(BaseMixin, Base):
  __tablename__ = "singers"

  first_name = Column(String(100))
  last_name = Column(String(200))
  full_name = Column(String,
                     server_default=FetchedValue(),
                     server_onupdate=FetchedValue())
  active = Column(Boolean)
  albums = relationship("Album", back_populates="singer")

  __mapper_args__ = {
    "eager_defaults": True,
    "version_id_col": BaseMixin.version_id
  }

  def __repr__(self):
    return (
      f"singers("
      f"id={self.id!r},"
      f"first_name={self.first_name!r},"
      f"last_name={self.last_name!r},"
      f"active={self.active!r},"
      f"created_at={format_timestamp(self.created_at)!r},"
      f"updated_at={format_timestamp(self.updated_at)!r}"
      f")"
    )


class Album(BaseMixin, Base):
  __tablename__ = "albums"

  title = Column(String(200))
  marketing_budget = Column(Numeric)
  release_date = Column(Date)
  cover_picture = Column(LargeBinary)
  singer_id = Column(String, ForeignKey("singers.id"))
  singer = relationship("Singer", back_populates="albums")
  # The `tracks` relationship uses passive_deletes=True, because `tracks` is
  # interleaved in `albums` with `ON DELETE CASCADE`. This prevents SQLAlchemy
  # from deleting the related tracks when an album is deleted, and lets the
  # database handle it.
  tracks = relationship("Track", back_populates="album", passive_deletes=True)

  def __repr__(self):
    return (
      f"albums("
      f"id={self.id!r},"
      f"title={self.title!r},"
      f"marketing_budget={self.marketing_budget!r},"
      f"release_date={self.release_date!r},"
      f"cover_picture={self.cover_picture!r},"
      f"singer={self.singer_id!r},"
      f"created_at={format_timestamp(self.created_at)!r},"
      f"updated_at={format_timestamp(self.updated_at)!r}"
      f")"
    )


class Track(BaseMixin, Base):
  __tablename__ = "tracks"

  id = Column(String, ForeignKey("albums.id"), primary_key=True)
  track_number = Column(Integer, primary_key=True)
  title = Column(String)
  sample_rate = Column(Float)
  album = relationship("Album", back_populates="tracks")

  def __repr__(self):
    return (
      f"tracks("
      f"id={self.id!r},"
      f"track_number={self.track_number!r},"
      f"title={self.title!r},"
      f"sample_rate={self.sample_rate!r},"
      f"created_at={format_timestamp(self.created_at)!r},"
      f"updated_at={format_timestamp(self.updated_at)!r}"
      f")"
    )


class Venue(BaseMixin, Base):
  __tablename__ = "venues"

  name = Column(String(200))
  description = Column(JSONB)

  def __repr__(self):
    return (
      f"venues("
      f"id={self.id!r},"
      f"name={self.name!r},"
      f"description={self.description!r},"
      f"created_at={format_timestamp(self.created_at)!r},"
      f"updated_at={format_timestamp(self.updated_at)!r}"
      f")"
    )


class Concert(BaseMixin, Base):
  __tablename__ = "concerts"

  name = Column(String(200))
  venue_id = Column(String, ForeignKey("venues.id"))
  venue = relationship("Venue")
  singer_id = Column(String, ForeignKey("singers.id"))
  singer = relationship("Singer")
  start_time = Column(DateTime(timezone=True))
  end_time = Column(DateTime(timezone=True))

  def __repr__(self):
    return (
      f"concerts("
      f"id={self.id!r},"
      f"name={self.name!r},"
      f"venue={self.venue!r},"
      f"singer={self.singer!r},"
      f"start_time={self.start_time!r},"
      f"end_time={self.end_time!r},"
      f"created_at={format_timestamp(self.created_at)!r},"
      f"updated_at={format_timestamp(self.updated_at)!r}"
      f")"
    )
