from sqlalchemy import insert
from connect import create_test_engine
from user_metadata import user_table

stmt = insert(user_table).values(name="spongebob", fullname="Spongebob "
                                                            "Squarepants")
print(stmt)
compiled = stmt.compile()
print(compiled.params)

engine = create_test_engine()
with engine.connect() as conn:
  result = conn.execute(stmt)
  print("Result: {}".format(result.all()))
  print("Row count: {}".format(result.rowcount))
  print("Inserted primary key: {}".format(result.inserted_primary_key))
  result = conn.execute(
    insert(user_table),
    [
      {"name": "sandy", "fullname": "Sandy Cheeks"},
      {"name": "patrick", "fullname": "Patrick Star"},
    ],
  )
  conn.commit()
  print("Row count: {}".format(result.rowcount))
