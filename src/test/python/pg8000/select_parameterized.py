import connect
import pg8000.dbapi

pg8000.dbapi.paramstyle = "named"

with connect.create_conn() as conn:
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_types WHERE col_bigint=:id", {"id": 1})
    results = cursor.fetchall()
    for row in results:
        print("first execution: %s" % row)

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_types WHERE col_bigint=:id", {"id": 1})
    results = cursor.fetchall()
    for row in results:
        print("second execution: %s" % row)
