from connect import create_test_engine
from user_metadata import user_table
from sqlalchemy import select


stmt = select(user_table).where(user_table.c.name == "spongebob")
print(stmt)

engine = create_test_engine()
with engine.connect() as conn:
  for row in conn.execute(stmt):
    print(row)
