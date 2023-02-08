from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes

engine = create_test_engine(options="?options=-c timezone=UTC")
session = Session(engine)
row = session.get(AllTypes, 1)
row.col_varchar = "updated string"
session.flush()
print("Before rollback: {}".format(row))
session.rollback()
print("After rollback: {}".format(row))
