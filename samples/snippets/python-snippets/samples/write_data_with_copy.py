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

# [START spanner_copy_from_stdin]
import os
import string
import psycopg


def write_data_with_copy(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:

        script_dir = os.path.dirname(os.path.abspath(__file__))
        singers_file_path = os.path.join(script_dir, "singers_data.txt")
        albums_file_path = os.path.join(script_dir, "albums_data.txt")

        conn.autocommit = True
        block_size = 1024
        with conn.cursor() as cur:
            with open(singers_file_path, "r") as f:
                with cur.copy("COPY singers (singer_id, first_name, last_name) "
                              "FROM STDIN") as copy:
                    while data := f.read(block_size):
                        copy.write(data)
            print("Copied %d singers" % cur.rowcount)

            with open(albums_file_path, "r") as f:
                with cur.copy("COPY albums "
                              "FROM STDIN") as copy:
                    while data := f.read(block_size):
                        copy.write(data)
            print("Copied %d albums" % cur.rowcount)
# [END spanner_copy_from_stdin]

if __name__ == "__main__":
    import sample_runner
    
    sample_runner.parse_arguments()
    write_data_with_copy(sample_runner.host, sample_runner.port, sample_runner.database)
