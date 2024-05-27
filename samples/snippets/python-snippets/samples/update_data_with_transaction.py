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

# [START spanner_dml_getting_started_update]
import string
import psycopg


def update_data_with_transaction(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        # Set autocommit=False to use transactions.
        # The first statement that is executed starts the transaction.
        conn.autocommit = False
        with conn.cursor() as cur:
            # Transfer marketing budget from one album to another.
            # We do it in a transaction to ensure that the transfer is atomic.
            # There is no need to explicitly start the transaction. The first
            # statement on the connection will start a transaction when
            # AutoCommit=false.
            select_marketing_budget_sql = ("SELECT marketing_budget "
                                           "from albums "
                                           "WHERE singer_id = %s "
                                           "and album_id = %s")
            # Get the marketing budget of Album #2.
            cur.execute(select_marketing_budget_sql, (2, 2))
            album2_budget = cur.fetchone()[0]
            transfer = 200000
            if album2_budget > transfer:
                # Get the marketing budget of Album #1.
                cur.execute(select_marketing_budget_sql, (1, 1))
                album1_budget = cur.fetchone()[0]
                # Transfer the marketing budgets and write the update back
                # to the database.
                album1_budget += transfer
                album2_budget -= transfer
                update_sql = ("update albums "
                              "set marketing_budget = %s "
                              "where singer_id = %s "
                              "and   album_id = %s")
                # Use a pipeline to execute two DML statements in one batch.
                with conn.pipeline():
                    cur.execute(update_sql, (album1_budget, 1, 1,))
                    cur.execute(update_sql, (album2_budget, 2, 2,))
            else:
                print("Insufficient budget to transfer")
        # Commit the transaction.
        conn.commit()
        print("Transferred marketing budget from Album 2 to Album 1")
# [END spanner_dml_getting_started_update]

import sample_runner

sample_runner.parse_arguments()
update_data_with_transaction(sample_runner.host, sample_runner.port, sample_runner.database)
