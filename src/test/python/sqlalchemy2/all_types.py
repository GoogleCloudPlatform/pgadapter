from datetime import datetime, timezone

import pytz
from sqlalchemy import Column, Integer, String, Boolean, LargeBinary, Float,\
  Numeric, DateTime, Date
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import registry

mapper_registry = registry()
Base = mapper_registry.generate_base()


class AllTypes(Base):
  __tablename__ = "all_types"

  col_bigint = Column(Integer, primary_key=True)
  col_bool = Column(Boolean)
  col_bytea = Column(LargeBinary)
  col_float8 = Column(Float)
  col_int = Column(Integer)
  col_numeric = Column(Numeric)
  col_timestamptz = Column(DateTime(timezone=True))
  col_date = Column(Date)
  col_varchar = Column(String)
  col_jsonb = Column(JSONB)

  col_array_bigint = Column(ARRAY(Integer))
  col_array_bool = Column(ARRAY(Boolean))
  col_array_bytea = Column(ARRAY(LargeBinary))
  col_array_float8 = Column(ARRAY(Float))
  col_array_int = Column(ARRAY(Integer))
  col_array_numeric = Column(ARRAY(Numeric))
  col_array_timestamptz = Column(ARRAY(DateTime(timezone=True)))
  col_array_date = Column(ARRAY(Date))
  col_array_varchar = Column(ARRAY(String))
  col_array_jsonb = Column(ARRAY(JSONB))

  def __repr__(self):
    return f"AllTypes(" \
           f"col_bigint=     {self.col_bigint!r}," \
           f"col_bool=       {self.col_bool!r}," \
           f"col_bytea=      {self.col_bytea!r}" \
           f"col_float8=     {self.col_float8!r}" \
           f"col_int=        {self.col_int!r}" \
           f"col_numeric=    {self.col_numeric!r}" \
           f"col_timestamptz={format_timestamp(self.col_timestamptz)!r}" \
           f"col_date=       {self.col_date!r}" \
           f"col_varchar=    {self.col_varchar!r}" \
           f"col_jsonb=      {self.col_jsonb!r}" \
           f"col_array_bigint=     {self.col_array_bigint!r}" \
           f"col_array_bool=       {self.col_array_bool!r}" \
           f"col_array_bytea=      {self.col_array_bytea!r}" \
           f"col_array_float8=     {self.col_array_float8!r}" \
           f"col_array_int=        {self.col_array_int!r}" \
           f"col_array_numeric=    {self.col_array_numeric!r}" \
           f"col_array_timestamptz={format_timestamps(self.col_array_timestamptz)!r}" \
           f"col_array_date=       {self.col_array_date!r}" \
           f"col_array_varchar=    {self.col_array_varchar!r}" \
           f"col_array_jsonb=      {self.col_array_jsonb!r}" \
           f")"


def format_timestamp(timestamp: datetime) -> str:
  return timestamp.astimezone(timezone.utc).isoformat() if timestamp else None


def format_timestamps(timestamps):
  if timestamps:
    return list(map(lambda x: x.astimezone(pytz.UTC) if x else None, timestamps))
  return None
