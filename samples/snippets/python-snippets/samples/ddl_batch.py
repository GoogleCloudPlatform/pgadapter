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

# [START spanner_ddl_batch]
import string
import psycopg


def ddl_batch(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        # DDL can only be executed when autocommit=True.
        conn.autocommit = True
        # Use a pipeline to batch multiple statements together.
        # Executing multiple DDL statements as one batch is
        # more efficient than executing each statement
        # individually.
        with conn.pipeline():
            # The following statements are buffered on PGAdapter
            # until the pipeline ends.
            conn.execute("CREATE TABLE venues ("
                         "  venue_id    bigint not null primary key,"
                         "  name        varchar(1024),"
                         "  description jsonb"
                         ")")
            conn.execute("CREATE TABLE concerts ("
                         "  concert_id bigint not null primary key ,"
                         "  venue_id   bigint not null,"
                         "  singer_id  bigint not null,"
                         "  start_time timestamptz,"
                         "  end_time   timestamptz,"
                         "  constraint fk_concerts_venues foreign key"
                         "    (venue_id) references venues (venue_id),"
                         "  constraint fk_concerts_singers foreign key"
                         "    (singer_id) references singers (singer_id)"
                         ")")
        print("Added venues and concerts tables")
# [END spanner_ddl_batch]

import sample_runner

sample_runner.parse_arguments()
ddl_batch(sample_runner.host, sample_runner.port, sample_runner.database)
