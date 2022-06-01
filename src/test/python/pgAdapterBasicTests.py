import psycopg2 as pg
import sys

try:
  connection = pg.connect(user = "postgres",
                          database = "postgres",
                          host = "localhost",
                          port = "5432")
  assert len(sys.argv) == 2

  cursor = connection.cursor()

  cursor.execute(sys.argv[1])

  for row in cursor:
    print(row)

except:

  print("Can't connect to the PG Adapter")



