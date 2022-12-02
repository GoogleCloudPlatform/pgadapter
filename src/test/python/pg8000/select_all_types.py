import connect


with connect.create_conn() as conn:
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_types")
    results = cursor.fetchall()
    for row in results:
        print("row: %s" % row)
