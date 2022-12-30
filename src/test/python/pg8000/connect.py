import pg8000.dbapi
import sys


def create_conn():
    host = sys.argv[1]
    port = sys.argv[2]
    if host.startswith("/"):
        conn = pg8000.dbapi.connect(unix_sock=host + "/.s.PGSQL." + port,
                                    database="d",
                                    user="test",
                                    password="test")
    else:
        conn = pg8000.dbapi.connect(host=host,
                                    port=port,
                                    database="d",
                                    user="test",
                                    password="test")
    conn.execute_simple("set time zone 'utc'")
    return conn

