from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes


engine = create_test_engine()
session = Session(engine)
row = AllTypes(
    col_bigint=1,
    col_bool=None,
    col_bytea=None,
    col_float8=None,
    col_int=None,
    col_numeric=None,
    col_timestamptz=None,
    col_date=None,
    col_varchar=None,
    col_jsonb=None,
)
session.add(row)
session.commit()
print("Inserted 1 row(s)")
