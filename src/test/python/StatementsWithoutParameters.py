''' Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

import psycopg2 as pg
import sys


def create_connection(version, host, port):
  try:
    connection = pg.connect(database="my-database",
                            host=host,
                            port=port,
                            options="-c server_version=" + version)
    connection.autocommit = True
    return connection
  except Exception as e:
    print(e)
    return None


def execute_query(sql, version, host, port):
  connection = create_connection(version, host, port)
  if connection is None:
    return
  try:
    cursor = connection.cursor()
    cursor.execute(sql)
    for row in cursor:
      print(row)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


def execute_update(sql, version, host, port):
  connection = create_connection(version, host, port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    cursor.execute(sql)
    print(cursor.rowcount)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


if __name__ == '__main__':
  assert len(sys.argv) >= 6

  version = sys.argv[1]
  host = sys.argv[2]
  port = sys.argv[3]
  statement_type = sys.argv[4]
  sql = sys.argv[5]

  if statement_type == 'query':
    execute_query(sql, version, host, port)
  elif statement_type == 'update':
    execute_update(sql, version, host, port)
  else:
    print('Invalid Statement Type')
