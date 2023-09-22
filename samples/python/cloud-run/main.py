import os
import psycopg
from flask import Flask

project = os.environ['SPANNER_PROJECT']
instance = os.environ['SPANNER_INSTANCE']
database = os.environ['SPANNER_DATABASE']
app = Flask(__name__)


@app.route("/")
def hello_world():
  # Connect to Cloud Spanner using psycopg3.
  # Note that we use the fully qualified database name to connect to the
  # database, as the Dockerfile startup script starts PGAdapter without a
  # default project or instance.
  with psycopg.connect("host=localhost port={port} "
                       "dbname=projects/{project}/instances/{instance}/databases/{database} "
                       "sslmode=disable"
                           .format(port=5432,
                                   project=project,
                                   instance=instance,
                                   database=database)) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      cur.execute("select 'Hello world!' as hello")
      return "Greeting from Cloud Spanner PostgreSQL: {greeting}\n"\
        .format(greeting=cur.fetchone()[0])


if __name__ == "__main__":
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
