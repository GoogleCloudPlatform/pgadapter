""" Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

from connect import create_test_engine
from sqlalchemy import text

engine = create_test_engine()
with engine.begin() as tx:
  result = tx.execute(text("INSERT INTO test VALUES (:x, :y)"),
                      [{"x": 3, "y": 'Three'}, {"x": 4, "y": 'Four'}])
  # This prints out the total row count, so in this case 2.
  print("Row count: {}".format(result.rowcount))
