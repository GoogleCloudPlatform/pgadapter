from sqlalchemy import Column, Integer, String, Boolean, LargeBinary, Float,\
  Numeric, DateTime, Date
from sqlalchemy.dialects.postgresql import JSONB
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

  def __repr__(self):
    return f"AllTypes(" \
           f"col_bigint=     {self.col_bigint!r}," \
           f"col_bool=       {self.col_bool!r}," \
           f"col_bytea=      {self.col_bytea!r}" \
           f"col_float8=     {self.col_float8!r}" \
           f"col_int=        {self.col_int!r}" \
           f"col_numeric=    {self.col_numeric!r}" \
           f"col_timestamptz={self.col_timestamptz!r}" \
           f"col_date=       {self.col_date!r}" \
           f"col_varchar=    {self.col_varchar!r}" \
           f"col_jsonb=      {self.col_jsonb!r}" \
           f")"
