import decimal

from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes
from datetime import datetime, date


engine = create_test_engine()
session = Session(engine)
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
  col_jsonb={"key1": "value1", "key2": "value2"},
  col_array_bigint=[1, None, 2],
  col_array_bool=[True, None, False],
  col_array_bytea=[bytes("bytes1", "utf-8"), None, bytes("bytes2", "utf-8")],
  col_array_float4=[-3.14, None, 99.99],
  col_array_float8=[-3.14, None, 99.99],
  col_array_int=[-100, None, -200],
  col_array_numeric=[decimal.Decimal("-6.626"), None, decimal.Decimal("99.99")],
  col_array_timestamptz=[
    datetime.fromisoformat("2010-11-08T18:33:12+01:00"),
    None,
    datetime.fromisoformat("2012-05-05T01:05:23.123+02:00"),
  ],
  col_array_date=[
    date.fromisoformat("2010-11-08"),
    None,
    date.fromisoformat("2012-05-05")
  ],
  col_array_varchar=["string1", None, "string2"],
  col_array_jsonb=[
    {"key1": "value1", "key2": "value2"},
    None,
    {"key1": "value3", "key2": "value4"}
  ],
)
session.add(row)
session.commit()
print("Inserted 1 row(s)")
