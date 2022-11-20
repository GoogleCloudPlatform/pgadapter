from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session

engine = create_engine(
  "postgresql+psycopg2://user:password@localhost:5433/knut-test-db",
  echo=True, future=True)
with engine.connect() as conn:
  result = conn.execute(text("select 'hello world'"))
  print(result.all())

with engine.connect() as conn:
  conn.execute(text("DELETE FROM test"))
  conn.execute(text("INSERT INTO test VALUES (:x, :y)"),
               [{"x": 1, "y": 'One'}, {"x": 2, "y": 'Two'}])
  conn.commit()

with engine.begin() as tx:
  tx.execute(text("INSERT INTO test VALUES (:x, :y)"),
               [{"x": 3, "y": 'Three'}, {"x": 4, "y": 'Four'}])

with engine.connect() as conn:
  result = conn.execute(text("SELECT id, value FROM test"))
  for row in result:
    print(f"x: {row.id}  y: {row.value}")

with engine.connect() as conn:
  result = conn.execute(text("SELECT id, value FROM test WHERE id > :x"), {"x": 2})
  for row in result:
    print(f"x: {row.id}  y: {row.value}")

stmt = text("SELECT id, value FROM test WHERE id > :x ORDER BY id, value")
with Session(engine) as session:
  result = session.execute(stmt, {"x": 3})
  for row in result:
    print(f"x: {row.id}  y: {row.value}")

with Session(engine) as session:
  result = session.execute(
  text("UPDATE test SET value=:y WHERE id=:x"),
    [{"x": 1, "y": 'one'}, {"x": 2, "y": 'two'}])
  session.commit()
