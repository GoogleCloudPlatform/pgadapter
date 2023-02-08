from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes


stale_read_engine = create_test_engine(
  autocommit=True,
  options="?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'")

session = Session(stale_read_engine)

row1 = session.get(AllTypes, 1)
print(row1)

row2 = session.get(AllTypes, 2)
print(row2)
