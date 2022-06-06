import psycopg2 as pg
import sys

try:
  assert len(sys.argv) == 3
  connection = pg.connect(user = "postgres",
                          database = "postgres",
                          host = "localhost",
                          port = sys.argv[1])
  cursor = connection.cursor()
  cursor.execute(sys.argv[2])
  for row in cursor:
    print(row)

except Exception as e:
  print(e)



