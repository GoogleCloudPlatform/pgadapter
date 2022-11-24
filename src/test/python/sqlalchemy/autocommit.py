from connect import create_test_engine
from user_metadata import user_table
from sqlalchemy import select

stmt = select(user_table).where(user_table.c.name == "spongebob")
engine = create_test_engine(autocommit=True)
with engine.connect() as conn:
  print(conn.get_isolation_level())
  for row in conn.execute(stmt):
    print(row)
