import decimal

from sqlalchemy.orm import Session
from sqlalchemy import exc
from connect import create_test_engine
from all_types import AllTypes
from datetime import datetime, date


engine = create_test_engine()
with Session(engine) as session:
  row = AllTypes(
      col_bigint=1,
      col_bool=True,
      col_bytea=bytes("test bytes", "utf-8"),
      col_float4=3.14,
      col_float8=3.14,
      col_int=100,
      col_numeric=decimal.Decimal("6.626"),
      col_timestamptz=datetime.fromisoformat("2011-11-04T00:05:23.123456+00:00"),
      col_date=date.fromisoformat("2011-11-04"),
      col_varchar="test string",
      col_jsonb={"key1": "value1", "key2": "value2"}
  )
  session.add(row)
  try:
    session.flush()
    print("Inserted 1 row(s)")
    session.commit()
  except exc.SQLAlchemyError as e:
    print("Insert failed: {}".format(e))
    # Rolling back a transaction that has failed is allowed. Any other
    # database operation would fail.
    session.rollback()
    pass
