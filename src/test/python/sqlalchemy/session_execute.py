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
from sqlalchemy.orm import Session

engine = create_test_engine()
with Session(engine) as session:
  result = session.execute(
    text("UPDATE test SET value=:y WHERE id=:x"),
    [{"x": 1, "y": 'one'}, {"x": 2, "y": 'two'}])
  print("Row count: {}".format(result.rowcount))
  session.commit()
