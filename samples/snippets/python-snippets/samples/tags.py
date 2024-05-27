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

# [START spanner_transaction_and_statement_tag]
import string
import psycopg


def tags(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        # Set autocommit=False to enable transactions.
        conn.autocommit = False
        with conn.cursor() as cur:
            # Set the TRANSACTION_TAG session variable to set a transaction tag
            # for the current transaction.
            cur.execute("set spanner.transaction_TAG='example-tx-tag'")

            # Set the STATEMENT_TAG session variable to set the request tag
            # that should be included with the next SQL statement.
            cur.execute("set spanner.statement_tag='query-marketing-budget'")

            singer_id = 1
            album_id = 1
            cur.execute("select marketing_budget "
                        "from albums "
                        "where singer_id = %s "
                        "  and album_id  = %s",
                        (singer_id, album_id,))
            marketing_budget = cur.fetchone()[0]

            # Reduce the marketing budget by 10% if it is more than 1,000.
            max_marketing_budget = 1000
            reduction = 0.1
            if marketing_budget > max_marketing_budget:
                # Make sure the marketing_budget remains an int.
                marketing_budget -= int(marketing_budget * reduction)
                # Set a statement tag for the update statement.
                cur.execute(
                    "set spanner.statement_tag='reduce-marketing-budget'")
                cur.execute("update albums set marketing_budget = %s "
                            "where singer_id = %s "
                            "  and album_id  = %s",
                            (marketing_budget, singer_id, album_id,))
            else:
                print("Marketing budget already less than or equal to 1,000")
        # Commit the transaction.
        conn.commit()
        print("Reduced marketing budget")
# [END spanner_transaction_and_statement_tag]

import sample_runner

sample_runner.parse_arguments()
tags(sample_runner.host, sample_runner.port, sample_runner.database)
