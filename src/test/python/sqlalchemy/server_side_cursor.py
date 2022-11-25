from connect import create_test_engine
from sqlalchemy import text


engine = create_test_engine()
with engine.connect() as conn:
  with conn.execution_options(yield_per=10).execute(
      text("select * from random")
  ) as result:
    for partition in result.partitions():
      # partition is an iterable that will be at most 10 items
      for row in partition:
        print(f"{row}")
