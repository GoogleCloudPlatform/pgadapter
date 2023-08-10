# Copyright 2023 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import psycopg
from flask import Flask

project = os.getenv('SPANNER_PROJECT', 'my-project')
instance = os.getenv('SPANNER_INSTANCE', 'my-instance')
database = os.getenv('SPANNER_DATABASE', 'my-database')

pgadapter_host = os.getenv('PGADAPTER_HOST', 'localhost')
pgadapter_port = os.getenv('PGADAPTER_PORT', '5432')

app = Flask(__name__)


@app.route("/")
def hello_world():
  # Connect to Cloud Spanner using psycopg3. psycopg3 is recommended above
  # psycopg2, as it uses server-side query parameters, which will give you
  # better performance.
  # Note that we use the fully qualified database name to connect to the
  # database, as PGAdapter is started without a default project or instance.
  with psycopg.connect("host={host} port={port} "
                       "dbname=projects/{project}/instances/{instance}/databases/{database} "
                       "sslmode=disable"
                           .format(host=pgadapter_host,
                                   port=pgadapter_port,
                                   project=project,
                                   instance=instance,
                                   database=database)) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      cur.execute("select 'Hello world!' as hello")
      return "Greeting from Cloud Spanner PostgreSQL using psycopg3: {greeting}\n"\
        .format(greeting=cur.fetchone()[0])


if __name__ == "__main__":
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
