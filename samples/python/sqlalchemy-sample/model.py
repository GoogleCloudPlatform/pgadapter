from sqlalchemy import Column, Integer, String, Boolean, LargeBinary, Float, \
  Numeric, DateTime, Date, FetchedValue, ForeignKey
from sqlalchemy.orm import registry, relationship

mapper_registry = registry()
Base = mapper_registry.generate_base()


class BaseMixin(object):
  id = Column(String, primary_key=True)
  created_at = Column(DateTime(timezone=True))
  updated_at = Column(DateTime(timezone=True))


class Singer(BaseMixin, Base):
  __tablename__ = "singers"

  first_name = Column(String(100))
  last_name = Column(String(200))
  full_name = Column(String, server_default=FetchedValue())
  active = Column(Boolean)
  albums = relationship("Album", back_populates="singer")

  def __repr__(self):
    return f"singers(" \
           f"id=        {self.id!r}," \
           f"first_name={self.first_name!r}," \
           f"last_name= {self.last_name!r}" \
           f"active=    {self.active!r}" \
           f")"


class Album(BaseMixin, Base):
  __tablename__ = "albums"

  title = Column(String(200))
  marketing_budget = Column(Numeric)
  release_date = Column(Date)
  cover_picture = Column(LargeBinary)
  singer_id = Column(String, ForeignKey("singers.id"))
  singer = relationship("Singer", back_populates="albums")
  tracks = relationship("Track", back_populates="album")

  def __repr__(self):
    return f"albums(" \
           f"id=              {self.id!r}," \
           f"title=           {self.title!r}," \
           f"marketing_budget={self.marketing_budget!r}" \
           f"release_date=    {self.release_date!r}" \
           f"cover_picture=   {self.cover_picture!r}" \
           f"singer=          {self.singer!r}" \
           f")"


class Track(BaseMixin, Base):
  __tablename__ = "tracks"

  id  = Column(String, ForeignKey("albums.id"), primary_key=True)
  track_number = Column(Integer, primary_key=True)
  title = Column(String)
  sample_rate = Column(Float)
  album = relationship("Album", back_populates="tracks")

  def __repr__(self):
    return f"tracks(" \
           f"id=          {self.id!r}," \
           f"track_number={self.track_number!r}," \
           f"title=       {self.title!r}," \
           f"sample_rate= {self.sample_rate!r}" \
           f"album=       {self.album!r}" \
           f")"


class Venue(BaseMixin, Base):
  __tablename__ = "venues"

  name = Column(String(200))
  description = Column(String)

  def __repr__(self):
    return f"venues(" \
           f"id=         {self.id!r}," \
           f"name=       {self.name!r}," \
           f"description={self.description!r}" \
           f")"


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
    return f"concerts(" \
           f"id=        {self.id!r}," \
           f"name=      {self.name!r}," \
           f"venue=     {self.venue!r}" \
           f"singer=    {self.singer!r}" \
           f"start_time={self.start_time!r}" \
           f"end_time=  {self.end_time!r}" \
           f")"
