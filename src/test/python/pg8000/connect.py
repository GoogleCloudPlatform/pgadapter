import pg8000.dbapi
import sys


def create_conn():
    host = sys.argv[1]
    port = sys.argv[2]
    if host.startswith("/"):
        return pg8000.dbapi.connect(unix_sock=host + "/.s.PGSQL." + port,
                                    database="d",
                                    user="test",
                                    password="test")
    return pg8000.dbapi.connect(host=host,
                                port=port,
                                database="d",
                                user="test",
                                password="test")

