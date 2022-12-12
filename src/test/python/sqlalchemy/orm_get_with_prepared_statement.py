from sqlalchemy import event
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from connect import create_test_engine
from all_types import AllTypes


engine = create_test_engine(options="?options=-c timezone=UTC")

# Register an event listener for this engine that creates a prepared statement
# for each connection that is created.
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
  cursor_obj = dbapi_connection.cursor()
  cursor_obj.execute("prepare get_all_types as select * from all_types where col_bigint=$1")
  cursor_obj.close()


def get_all_types(col_bigint):
  return session.query(AllTypes) \
    .from_statement(text("execute get_all_types (:col_bigint)")) \
    .params(col_bigint=col_bigint) \
    .first()


session = Session(engine)
row = get_all_types(1)
print(row)
