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
from io import StringIO

def create_connection(port):
  try:
    connection = pg.connect(user="postgres",
                            database="postgres",
                            host="localhost",
                            port=port)
    connection.autocommit = True
    return connection
  except Exception as e:
    print(e)
    return None

def execute_copy_from(sql, file, cursor):
  try:
    f = StringIO(file)
    cursor.copy_expert(sql, f)
    print(cursor.rowcount)
  except Exception as e:
    print(e)

def execute_simple_copy_from(file, cursor):
  try:
    f = StringIO(file)
    cursor.copy_from(f, 'test')
    print(cursor.rowcount)
  except Exception as e:
    print(e)

def execute_copy_to(sql, cursor):
  try:
    cursor.copy_expert(sql, sys.stdout)
  except Exception as e:
    print(e)

def execute_simple_copy_to(cursor):
  try:
    cursor.copy_to(sys.stdout, 'test')
  except Exception as e:
    print(e)

def execute_copy(sql, file, port, copy_type):
  connection = create_connection(port)
  if connection is None:
    return
  try:
    cursor = connection.cursor()
    if copy_type == 'FROM':
      execute_copy_from(sql, file, cursor)
    elif copy_type == 'SIMPLE_FROM':
      execute_simple_copy_from(file, cursor)
    elif copy_type == 'TO':
      execute_copy_to(sql, cursor)
    elif copy_type == 'SIMPLE_TO':
      execute_simple_copy_to(cursor)
    else:
      print('Invalid Copy Type')
      return
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()

if __name__ == '__main__':
  assert len(sys.argv) == 5

  port = sys.argv[1]
  sql = sys.argv[2]
  file = sys.argv[3]
  copy_type = sys.argv[4]

  execute_copy(sql, file, port, copy_type)