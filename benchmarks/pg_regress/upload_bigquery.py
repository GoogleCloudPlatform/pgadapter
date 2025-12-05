# Copyright 2025 Google LLC
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

import json
import os
import sys
import uuid

from google.cloud import bigquery


def main():
    """Main function to upload test results to bigquery."""
    if len(sys.argv) != 5:
        print(
            "Usage: python upload_bigquery.py <json_result_file> <bigquery_table> <test_env> <timestamp>"
        )
        sys.exit(1)

    json_file_path = sys.argv[1]
    bq_table = sys.argv[2]
    test_env = sys.argv[3]
    timestamp = sys.argv[4]

    # Read a json file
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)

    # Only get the first 8 characters.
    run_id = uuid.uuid4().hex[:8]
    rows_to_insert = [{
        "run_id": run_id,
        "test_name": "overall",
        "target_env": test_env,
        "passed": data["overall"]["passed"],
        "total": data["overall"]["total"],
        "time": timestamp
    }]
    for key, item in data["files"].items():
        rows_to_insert.append({
            "run_id": run_id,
            "test_name": key,
            "target_env": test_env,
            "passed": item["passed"],
            "total": item["total"],
            "time": timestamp
        })

    client = bigquery.Client()

    errors = client.insert_rows_json(bq_table, rows_to_insert)
    if errors:
        print(f"Errors occurred during insertion: {errors}")
    else:
        print(f"Inserted {len(rows_to_insert)} rows.")


if __name__ == "__main__":
    main()
