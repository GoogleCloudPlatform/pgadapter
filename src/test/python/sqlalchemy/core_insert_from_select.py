from sqlalchemy import insert, select
from user_metadata import user_table, address_table
from connect import create_test_engine

select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
insert_stmt = insert(address_table).from_select(
  ["user_id", "email_address"], select_stmt
)
print(insert_stmt)

select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
insert_stmt = insert(address_table).from_select(
  ["user_id", "email_address"], select_stmt
)

engine = create_test_engine()
with engine.connect() as conn:
  res = conn.execute(insert_stmt.returning(address_table.c.id,
                                           address_table.c.email_address))
  print("Inserted rows: {}".format(res.rowcount))
  print("Returned rows: {}".format(res.all()))
