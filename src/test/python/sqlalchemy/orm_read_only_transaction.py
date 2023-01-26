from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes


engine = create_test_engine(options="?options=-c timezone=UTC")
readonly_engine = engine.execution_options(postgresql_readonly=True)

session = Session(readonly_engine)
session.begin()
row = session.get(AllTypes, 1)
print(row)
