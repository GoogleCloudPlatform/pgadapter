from sqlalchemy import select
from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes


engine = create_test_engine(options="?options=-c timezone=UTC")
session = Session(engine)
row = session.scalars(select(AllTypes)).first()
print(row)
