# Copyright 2024 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START spanner_statement_timeout]
import string
import psycopg
from psycopg import DatabaseError


def query_with_timeout(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Set the statement timeout that should be used for all statements
            # on this connection to 5 seconds.
            # Supported time units are 's' (seconds), 'ms' (milliseconds),
            # 'us' (microseconds), and 'ns' (nanoseconds).
            cur.execute("set statement_timeout='5s'")
            try:
                cur.execute("SELECT singer_id, album_id, album_title "
                            "FROM albums "
                            "WHERE album_title in ("
                            "  SELECT first_name "
                            "  FROM singers "
                            "  WHERE last_name LIKE '%a%'"
                            "     OR last_name LIKE '%m%'"
                            ")")
                for album in cur:
                    print(album)
            except DatabaseError as exception:
                print("Error occurred during query execution: %s" % exception)
# [END spanner_statement_timeout]

if __name__ == "__main__":
    import sample_runner
    
    sample_runner.parse_arguments()
    query_with_timeout(sample_runner.host, sample_runner.port, sample_runner.database)
