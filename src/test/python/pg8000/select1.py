import connect


with connect.create_conn() as conn:
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    print("SELECT 1: %s" % row)
