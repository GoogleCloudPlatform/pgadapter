import pg8000.dbapi

conn = pg8000.dbapi.connect(host="localhost",
                            port=5433,
                            database="knut-test-db",
                            user="test",
                            password="test")
cursor = conn.cursor()
cursor.execute("DELETE FROM test")
print("delete rowcount = %s" % cursor.rowcount)

cursor = conn.cursor()
cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s), (%s, %s) RETURNING id, value",
               (1, "Ender's Game", 2, "Speaker for the Dead"))
results = cursor.fetchall()
for row in results:
  id, value = row
  print("id = %s, value = %s" % (id, value))

conn.commit()

cursor.execute("SELECT TIMESTAMPTZ '2021-10-10'")
row = cursor.fetchone()
print("Timestamp = %s" % row)

conn.close()

pg8000.dbapi.paramstyle = "numeric"
conn = pg8000.dbapi.connect(host="localhost",
                            port=5433,
                            database="knut-test-db",
                            user="test",
                            password="test")
cursor = conn.cursor()
cursor.execute("SELECT * FROM test WHERE id=:1", (1,))
row = cursor.fetchone()
print("Row: %s" % row)
